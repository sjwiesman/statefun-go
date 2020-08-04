package testing

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/assert"
	"statefun-go/pkg/flink/statefun"
	"testing"
	"time"
)

var caller = statefun.Address{
	Namespace: "apache",
	Type:      "caller",
	Id:        "id2",
}

var egress = statefun.Egress{"test", "egress"}

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

	functions := statefun.NewStatefulFunctions()

	functions.StatefulFunction(statefun.FunctionType{
		Namespace: "remote",
		Type:      "greeter",
	}, Greeter{})

	result, err := functions.Process(&toFunction)
	if err != nil {
		assert.Error(t, err)
	}

	response := result.GetInvocationResult()
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

type Greeter struct{}

func (f Greeter) Invoke(message *any.Any, ctx *statefun.Context) error {
	var count Counter
	if err := ctx.GetAndUnpack("counter", &count); err != nil {
		return err
	}

	count.Count += 1

	greeting := &Greeting{
		Greeting: "Hello",
	}

	if err := ctx.ReplyAndPack(greeting); err != nil {
		return err
	}

	if err := ctx.SendAfterAndPack(ctx.Caller(), time.Duration(6e+10), greeting); err != nil {
		return err
	}

	if err := ctx.SendEgressAndPack(egress, greeting); err != nil {
		return err
	}

	if err := ctx.SetAndPack("counter", &count); err != nil {
		return err
	}

	return nil
}
