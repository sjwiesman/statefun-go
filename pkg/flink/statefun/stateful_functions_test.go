package statefun

import (
	"bytes"
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/test"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/io"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var caller = messages.Address{
	Namespace: "remote",
	Type:      "caller",
	Id:        "id2",
}

var egress = io.EgressIdentifier{EgressNamespace: "test", EgressType: "egress"}

var serializedArgument any.Any

var serializedGreeting any.Any

var stateValue []byte

//noinspection GoVetCopyLock
func init() {
	pointer, _ := ptypes.MarshalAny(&test.Invoke{})
	serializedArgument = *pointer

	pointer, _ = ptypes.MarshalAny(&test.Greeting{Greeting: "Hello"})
	serializedGreeting = *pointer

	countAny, _ := ptypes.MarshalAny(&test.Counter{Count: 1})
	stateValue, _ = proto.Marshal(countAny)
}

//noinspection GoVetCopyLock
func TestFunctionHandler(t *testing.T) {
	toFunction := messages.ToFunction{
		Request: &messages.ToFunction_Invocation_{
			Invocation: &messages.ToFunction_InvocationBatchRequest{
				Target: &messages.Address{
					Namespace: "remote",
					Type:      "greeter",
					Id:        "id",
				},
				State: []*messages.ToFunction_PersistedValue{
					{
						StateName:  "modified-state",
						StateValue: stateValue,
					},
					{
						StateName:  "deleted-state",
						StateValue: stateValue,
					},
					{
						StateName:  "read-only-state",
						StateValue: stateValue,
					},
				},
				Invocations: []*messages.ToFunction_Invocation{
					{
						Caller: &messages.Address{
							Namespace: "remote",
							Type:      "caller",
							Id:        "id2",
						},
						Argument: &serializedArgument,
					},
				},
			},
		},
	}

	functions := NewFunctionRegistry()

	functions.RegisterFunction(FunctionType{
		Namespace: "remote",
		Type:      "greeter",
	}, Greeter{})

	server := httptest.NewServer(functions)
	defer server.Close()

	binary, _ := proto.Marshal(&toFunction)
	resp, err := http.Post(server.URL, "application/octet-stream", bytes.NewReader(binary))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "received non-200 response")

	var fromFunction messages.FromFunction
	respBytes, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	err = proto.Unmarshal(respBytes, &fromFunction)

	response := fromFunction.GetInvocationResult()

	mutations := map[string]*messages.FromFunction_PersistedValueMutation{}
	for _, mutation := range response.StateMutations {
		mutations[mutation.StateName] = mutation
	}

	assert.Equal(t, 2, len(mutations), "wrong number of state mutations")

	assert.Contains(t, mutations, "modified-state", "missing modified state")
	assert.Equal(t, messages.FromFunction_PersistedValueMutation_MODIFY, mutations["modified-state"].MutationType, "wrong mutation type")

	var packagedState any.Any
	if err := proto.Unmarshal(mutations["modified-state"].StateValue, &packagedState); err != nil {
		assert.Fail(t, err.Error())
	}

	var counterUpdate test.Counter
	if err := ptypes.UnmarshalAny(&packagedState, &counterUpdate); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.Equal(t, int32(2), counterUpdate.Count, "wrong counter value")

	assert.Contains(t, mutations, "deleted-state", "missing deleted state")
	assert.Equal(t, messages.FromFunction_PersistedValueMutation_DELETE, mutations["deleted-state"].MutationType, "wrong mutation type")

	assert.Equal(t, 1, len(response.OutgoingMessages), "wrong number of outgoing messages")
	assert.Equal(t, caller, *response.OutgoingMessages[0].Target, "wrong message target")
	assert.Equal(t, serializedGreeting, *response.OutgoingMessages[0].Argument, "wrong message argument")

	assert.Equal(t, 1, len(response.DelayedInvocations), "wrong number of delayed invocations")
	assert.Equal(t, caller, *response.DelayedInvocations[0].Target, "wrong message target")
	assert.Equal(t, int64(60000), response.DelayedInvocations[0].DelayInMs, "wrong message delay")
	assert.Equal(t, serializedGreeting, *response.DelayedInvocations[0].Argument, "wrong message argument")

	assert.Equal(t, 1, len(response.OutgoingEgresses), "wrong number of egress messages")
	assert.Equal(t, egress.EgressNamespace, response.OutgoingEgresses[0].EgressNamespace, "wrong egress namespace")
	assert.Equal(t, egress.EgressType, response.OutgoingEgresses[0].EgressType, "wrong egress type")
	assert.Equal(t, serializedGreeting, *response.OutgoingEgresses[0].Argument, "wrong egress message")
}

func TestValidation(t *testing.T) {
	functions := NewFunctionRegistry()
	server := httptest.NewServer(functions)
	defer server.Close()

	resp, _ := http.Get(server.URL)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "incorrect validation code on bad method")

	resp, _ = http.Post(server.URL, "application/json", nil)
	assert.Equal(t, http.StatusUnsupportedMediaType, resp.StatusCode, "incorrect validation code on bad media type")

	resp, _ = http.Post(server.URL, "application/octet-stream", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "incorrect validation code on missing content")

	resp, _ = http.Post(server.URL, "application/octet-stream", strings.NewReader("bad content"))
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "incorrect validation code on malformed content")
}

type Greeter struct{}

func (f Greeter) Invoke(ctx context.Context, runtime StatefulFunctionRuntime, msg *anypb.Any) error {
	if err := ptypes.UnmarshalAny(msg, &test.Invoke{}); err != nil {
		return err
	}

	var count test.Counter
	if err := runtime.Get("modified-state", &count); err != nil {
		return err
	}

	count.Count += 1

	greeting := &test.Greeting{
		Greeting: "Hello",
	}

	caller := Caller(ctx)
	if err := runtime.Send(caller, greeting); err != nil {
		return err
	}

	if err := runtime.SendAfter(caller, time.Duration(6e+10), greeting); err != nil {
		return err
	}

	if err := runtime.SendEgress(egress, greeting); err != nil {
		return err
	}

	if err := runtime.Set("modified-state", &count); err != nil {
		return err
	}

	runtime.Clear("deleted-state")

	if err := runtime.Get("read-only-state", &count); err != nil {
		return err
	}

	if count.Count != 1 {
		return errors.New("invalid count for read-only state")
	}

	return nil
}
