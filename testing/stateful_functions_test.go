package testing

import (
	"bytes"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"statefun-go/statefun"
	"strings"
	"testing"
	"time"
)

var caller = statefun.Address{
	Namespace: "remote",
	Type:      "caller",
	Id:        "id2",
}

var egress = statefun.EgressIdentifier{EgressNamespace: "test", EgressType: "egress"}

var serializedGreeting any.Any

//noinspection GoVetCopyLock
func init() {
	pointer, _ := ptypes.MarshalAny(&Greeting{Greeting: "Hello"})
	serializedGreeting = *pointer
}

//noinspection GoVetCopyLock
func TestFunctionHandler(t *testing.T) {
	var stateValue []byte
	if countAny, err := ptypes.MarshalAny(&Counter{Count: 1}); err != nil {
		assert.Fail(t, "Failed to initialize counter")
	} else if stateValue, err = proto.Marshal(countAny); err != nil {
		assert.Fail(t, "Failed to initialize counter")
	}

	argument, err := ptypes.MarshalAny(&Invoke{})
	if err != nil {
		assert.Fail(t, "Failed to initialize argument")
	}

	toFunction := statefun.ToFunction{
		Request: &statefun.ToFunction_Invocation_{
			Invocation: &statefun.ToFunction_InvocationBatchRequest{
				Target: &statefun.Address{
					Namespace: "remote",
					Type:      "greeter",
					Id:        "id",
				},
				State: []*statefun.ToFunction_PersistedValue{
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
				Invocations: []*statefun.ToFunction_Invocation{
					{
						Caller: &statefun.Address{
							Namespace: "remote",
							Type:      "caller",
							Id:        "id2",
						},
						Argument: argument,
					},
				},
			},
		},
	}

	functions := statefun.NewFunctionRegistry()

	functions.RegisterFunction(statefun.FunctionType{
		Namespace: "remote",
		Type:      "greeter",
	}, Greeter{})

	server := httptest.NewServer(functions)
	defer server.Close()

	binary, _ := proto.Marshal(&toFunction)
	resp, err := http.Post(server.URL, "application/octet-stream", bytes.NewReader(binary))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "received non-200 response")

	var fromFunction statefun.FromFunction
	respBytes, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)

	err = proto.Unmarshal(respBytes, &fromFunction)

	response := fromFunction.GetInvocationResult()

	mutations := map[string]*statefun.FromFunction_PersistedValueMutation{}
	for _, mutation := range response.StateMutations {
		mutations[mutation.StateName] = mutation
	}

	assert.Equal(t, 2, len(mutations), "wrong number of state mutations")

	assert.Contains(t, mutations, "modified-state", "missing modified state")
	assert.Equal(t, statefun.FromFunction_PersistedValueMutation_MODIFY, mutations["modified-state"].MutationType, "wrong mutation type")

	var packagedState any.Any
	if err := proto.Unmarshal(mutations["modified-state"].StateValue, &packagedState); err != nil {
		assert.Fail(t, err.Error())
	}

	var counterUpdate Counter
	if err := ptypes.UnmarshalAny(&packagedState, &counterUpdate); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.Equal(t, int32(2), counterUpdate.Count, "wrong counter value")

	assert.Contains(t, mutations, "deleted-state", "missing deleted state")
	assert.Equal(t, statefun.FromFunction_PersistedValueMutation_DELETE, mutations["deleted-state"].MutationType, "wrong mutation type")

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
	functions := statefun.NewFunctionRegistry()
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

func (f Greeter) Invoke(ctx statefun.StatefulFunctionIO, msg *any.Any) error {
	if err := ptypes.UnmarshalAny(msg, &Invoke{}); err != nil {
		return err
	}

	var count Counter
	if err := ctx.Get("modified-state", &count); err != nil {
		return err
	}

	count.Count += 1

	greeting := &Greeting{
		Greeting: "Hello",
	}

	if err := ctx.Reply(greeting); err != nil {
		return err
	}

	if err := ctx.SendAfter(ctx.Caller(), time.Duration(6e+10), greeting); err != nil {
		return err
	}

	if err := ctx.SendEgress(egress, greeting); err != nil {
		return err
	}

	if err := ctx.Set("modified-state", &count); err != nil {
		return err
	}

	ctx.Clear("deleted-state")

	if err := ctx.Get("read-only-state", &count); err != nil {
		return err
	}

	if count.Count != 1 {
		return errors.New("invalid count for read-only state")
	}

	return nil
}
