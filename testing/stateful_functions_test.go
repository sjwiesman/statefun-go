package testing

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"statefun-go/statefun"
	"strings"
	"testing"
	"time"
)

var caller = statefun.Address{
	Namespace: "apache",
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
						StateName:  "counter",
						StateValue: stateValue,
					},
				},
				Invocations: []*statefun.ToFunction_Invocation{
					{
						Caller: &statefun.Address{
							Namespace: "apache",
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

	if err != nil {
		assert.Error(t, err)
	}

	if resp.StatusCode != 200 {
		assert.Fail(t, "received non-200 response: %d\n", resp.StatusCode)
	}

	var fromFunction statefun.FromFunction
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		assert.Error(t, err)
	}

	err = proto.Unmarshal(respBytes, &fromFunction)

	response := fromFunction.GetInvocationResult()
	assert.Equal(t, 1, len(response.StateMutations), "Wrong number of state mutations")

	mutation := response.StateMutations[0]
	assert.Equal(t, "counter", mutation.StateName, "Wrong state mutated")
	assert.Equal(t, statefun.FromFunction_PersistedValueMutation_MODIFY, mutation.MutationType, "Wrong mutation type")

	var packagedState any.Any
	if err := proto.Unmarshal(mutation.StateValue, &packagedState); err != nil {
		assert.Fail(t, err.Error())
	}

	var counterUpdate Counter
	if err := ptypes.UnmarshalAny(&packagedState, &counterUpdate); err != nil {
		assert.Fail(t, err.Error())
	}

	assert.Equal(t, int32(2), counterUpdate.Count, "Wrong counter value")

	assert.Equal(t, 1, len(response.OutgoingMessages), "Wrong number of outgoing messages")
	assert.Equal(t, caller, *response.OutgoingMessages[0].Target, "Wrong message target")
	assert.Equal(t, serializedGreeting, *response.OutgoingMessages[0].Argument, "Wrong message argument")

	assert.Equal(t, 1, len(response.DelayedInvocations), "Wrong number of delayed invocations")
	assert.Equal(t, caller, *response.DelayedInvocations[0].Target, "Wrong message target")
	assert.Equal(t, int64(60000), response.DelayedInvocations[0].DelayInMs, "Wrong message delay")
	assert.Equal(t, serializedGreeting, *response.DelayedInvocations[0].Argument, "Wrong message argument")

	assert.Equal(t, 1, len(response.OutgoingEgresses), "Wrong number of egress messages")
	assert.Equal(t, egress.EgressNamespace, response.OutgoingEgresses[0].EgressNamespace, "Wrong egress namespace")
	assert.Equal(t, egress.EgressType, response.OutgoingEgresses[0].EgressType, "Wrong egress type")
	assert.Equal(t, serializedGreeting, *response.OutgoingEgresses[0].Argument, "Wrong egress message")
}

func TestValidation(t *testing.T) {
	functions := statefun.NewFunctionRegistry()
	server := httptest.NewServer(functions)
	defer server.Close()

	resp, _ := http.Get(server.URL)

	if resp.StatusCode != http.StatusMethodNotAllowed {
		assert.Fail(t, "incorrect validation code on bad method: %d\n", resp.StatusCode)
	}

	resp, _ = http.Post(server.URL, "application/json", nil)

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		assert.Fail(t, "incorrect validation code on bad media type: %d\n", resp.StatusCode)
	}

	resp, _ = http.Post(server.URL, "application/octet-stream", nil)

	if resp.StatusCode != http.StatusBadRequest {
		assert.Fail(t, "incorrect validation code on missing content: %d\n", resp.StatusCode)
	}

	resp, _ = http.Post(server.URL, "application/octet-stream", strings.NewReader("bad content"))

	if resp.StatusCode != http.StatusBadRequest {
		assert.Fail(t, "incorrect validation code on missing content: %d\n", resp.StatusCode)
	}
}

type Greeter struct{}

func (f Greeter) Invoke(ctx statefun.StatefulFunctionIO, _ *anypb.Any) error {
	var count Counter
	if err := ctx.Get("counter", &count); err != nil {
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

	if err := ctx.Set("counter", &count); err != nil {
		return err
	}

	return nil
}
