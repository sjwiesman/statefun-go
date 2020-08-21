package statefun

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/io"
	"time"
)

// Provides the effect tracker for a single StatefulFunction instance.
// The invocation's io context may be used to obtain the Address of itself or the calling
// function (if the function was invoked by another function), or used to invoke other functions
// (including itself) and to send messages to egresses. Additionally, it supports
// reading and writing persisted state values with exactly-once guarantees provided
// by the runtime.
type StatefulFunctionRuntime interface {

	// Get retrieves the state for the given name and
	// unmarshalls the encoded value contained into the provided message state.
	// It returns an error if the target message does not match the type
	// in the Any message or if an unmarshal error occurs.
	Get(name string, state proto.Message) error

	// Set stores the value under the given name in state and
	// marshals the given message m into an any.Any message
	// if it is not already.
	Set(name string, value proto.Message) error

	// Clear deletes the state registered under the name
	Clear(name string)

	// Invokes another function with an input, identified by the target function's Address
	// and marshals the given message into an any.Any.
	Send(target *Address, message proto.Message) error

	// Invokes another function with an input, identified by the target function's
	// FunctionType and unique id after a specified delay. This method is durable
	// and as such, the message will not be lost if the system experiences
	// downtime between when the message is sent and the end of the duration.
	// This method marshals the given message into an any.Any.
	SendAfter(target *Address, duration time.Duration, message proto.Message) error

	// Sends an output to an EgressIdentifier.
	// This method marshals the given message into an any.Any.
	SendEgress(egress io.EgressIdentifier, message proto.Message) error
}

// Tracks the state changes
// of the function invocations
type state struct {
	updated bool
	value   *any.Any
}

// runtime is the main effect tracker of the function invocation
// It tracks all responses that will be sent back to the
// Flink runtime after the full batch has been executed.
type runtime struct {
	states            map[string]*state
	invocations       []*messages.FromFunction_Invocation
	delayedInvocation []*messages.FromFunction_DelayedInvocation
	outgoingEgress    []*messages.FromFunction_EgressMessage
}

// Create a new runtime based on the target function
// and set of initial states.
func newStateFunIO(persistedValues []*messages.ToFunction_PersistedValue) *runtime {
	ctx := &runtime{
		states:            map[string]*state{},
		invocations:       []*messages.FromFunction_Invocation{},
		delayedInvocation: []*messages.FromFunction_DelayedInvocation{},
		outgoingEgress:    []*messages.FromFunction_EgressMessage{},
	}

	for _, persistedValue := range persistedValues {
		value := any.Any{}
		err := proto.Unmarshal(persistedValue.StateValue, &value)
		if err != nil {

		}

		ctx.states[persistedValue.StateName] = &state{
			updated: false,
			value:   &value,
		}
	}

	return ctx
}

func (tracker *runtime) Get(name string, state proto.Message) error {
	packedState := tracker.states[name]
	if packedState == nil {
		return errors.New(fmt.Sprintf("unknown state name %s", name))
	}

	if packedState.value == nil || packedState.value.TypeUrl == "" {
		return nil
	}

	return internal.Unmarshall(packedState.value, state)
}

func (tracker *runtime) Set(name string, value proto.Message) error {
	state := tracker.states[name]
	if state == nil {
		return errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	packedState, err := internal.Marshall(value)
	if err != nil {
		return err
	}

	state.updated = true
	state.value = packedState
	tracker.states[name] = state

	return nil
}

func (tracker *runtime) Clear(name string) {
	_ = tracker.Set(name, nil)
}

func (tracker *runtime) Send(target *Address, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedState, err := internal.Marshall(message)
	if err != nil {
		return err
	}

	invocation := &messages.FromFunction_Invocation{
		Target: &messages.Address{
			Namespace: target.FunctionType.Namespace,
			Type:      target.FunctionType.Type,
			Id:        target.Id,
		},
		Argument: packedState,
	}

	tracker.invocations = append(tracker.invocations, invocation)
	return nil
}

func (tracker *runtime) SendAfter(target *Address, duration time.Duration, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedMessage, err := internal.Marshall(message)
	if err != nil {
		return err
	}

	delayedInvocation := &messages.FromFunction_DelayedInvocation{
		Target: &messages.Address{
			Namespace: target.FunctionType.Namespace,
			Type:      target.FunctionType.Type,
			Id:        target.Id,
		},
		DelayInMs: duration.Milliseconds(),
		Argument:  packedMessage,
	}

	tracker.delayedInvocation = append(tracker.delayedInvocation, delayedInvocation)
	return nil
}

func (tracker *runtime) SendEgress(egress io.EgressIdentifier, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to egress")
	}

	packedMessage, err := internal.Marshall(message)
	if err != nil {
		return err
	}

	egressMessage := &messages.FromFunction_EgressMessage{
		EgressNamespace: egress.EgressNamespace,
		EgressType:      egress.EgressType,
		Argument:        packedMessage,
	}

	tracker.outgoingEgress = append(tracker.outgoingEgress, egressMessage)
	return nil
}

func (tracker *runtime) fromFunction() (*messages.FromFunction, error) {
	var mutations []*messages.FromFunction_PersistedValueMutation
	for name, state := range tracker.states {
		if !state.updated {
			continue
		}

		var err error
		var bytes []byte
		var mutationType messages.FromFunction_PersistedValueMutation_MutationType

		if state.value == nil {
			bytes = nil
			mutationType = messages.FromFunction_PersistedValueMutation_DELETE
		} else {
			mutationType = messages.FromFunction_PersistedValueMutation_MODIFY

			bytes, err = proto.Marshal(state.value)
			if err != nil {
				return nil, err
			}
		}

		mutation := &messages.FromFunction_PersistedValueMutation{
			MutationType: mutationType,
			StateName:    name,
			StateValue:   bytes,
		}

		mutations = append(mutations, mutation)
	}

	return &messages.FromFunction{
		Response: &messages.FromFunction_InvocationResult{
			InvocationResult: &messages.FromFunction_InvocationResponse{
				StateMutations:     mutations,
				OutgoingMessages:   tracker.invocations,
				DelayedInvocations: tracker.delayedInvocation,
				OutgoingEgresses:   tracker.outgoingEgress,
			},
		},
	}, nil
}
