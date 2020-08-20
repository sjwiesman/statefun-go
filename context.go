package statefun_go

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"time"
)

// Provides the effectTracker for a single StatefulFunction instance.
// The invocation's effectTracker may be used to obtain the {@link Address} of itself or the calling
// function (if the function was invoked by another function), or used to invoke other functions
// (including itself) and to send messages to egresses. Additionally, it supports
// reading and writing persisted state values with exactly-once guarantees provided
// by the effectTracker.
type StatefulFunctionIO interface {
	// Self returns the address of the current
	// function instance under evaluation
	Self() *Address

	// Caller returns the address of the caller function.
	// The caller may be nil if the message
	// was sent directly from an ingress
	Caller() *Address

	// Get retrieves the state for the given name and
	// unmarshals the encoded value contained into the provided message state.
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

	// Invokes the calling function of the current invocation under effectTracker. This has the same effect
	// as calling Send with the address obtained from Caller, and
	// will not work if the current function was not invoked by another function.
	// This method marshals the given message into an any.Any.
	Reply(message proto.Message) error

	// Invokes another function with an input, identified by the target function's
	// FunctionType and unique id after a specified delay. This method is durable
	// and as such, the message will not be lost if the system experiences
	// downtime between when the message is sent and the end of the duration.
	// This method marshals the given message into an any.Any.
	SendAfter(target *Address, duration time.Duration, message proto.Message) error

	// Sends an output to an EgressIdentifier.
	// This method marshals the given message into an any.Any.
	SendEgress(egress EgressIdentifier, message proto.Message) error
}

// Tracks the state changes
// of the function invocations
type state struct {
	updated bool
	value   *any.Any
}

// effectTracker is the main effect tracker of the function invocation
// It tracks all responses that will be sent back to the
// Flink effectTracker after the full batch has been executed.
type effectTracker struct {
	self              *Address
	caller            *Address
	states            map[string]*state
	invocations       []*FromFunction_Invocation
	delayedInvocation []*FromFunction_DelayedInvocation
	outgoingEgress    []*FromFunction_EgressMessage
}

// Create a new effectTracker based on the target function
// and set of initial states.
func newContext(self *Address, persistedValues []*ToFunction_PersistedValue) effectTracker {
	ctx := effectTracker{
		self:              self,
		caller:            nil,
		states:            map[string]*state{},
		invocations:       []*FromFunction_Invocation{},
		delayedInvocation: []*FromFunction_DelayedInvocation{},
		outgoingEgress:    []*FromFunction_EgressMessage{},
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

func (tracker *effectTracker) Self() *Address {
	return tracker.self
}

func (tracker *effectTracker) Caller() *Address {
	return tracker.caller
}

func (tracker *effectTracker) Get(name string, state proto.Message) error {
	packedState := tracker.states[name]
	if packedState == nil {
		return errors.New(fmt.Sprintf("unknown state name %s", name))
	}

	if packedState.value == nil || packedState.value.TypeUrl == "" {
		return nil
	}

	return unmarshall(packedState.value, state)
}

func (tracker *effectTracker) Set(name string, value proto.Message) error {
	state := tracker.states[name]
	if state == nil {
		return errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	packedState, err := marshall(value)
	if err != nil {
		return err
	}

	state.updated = true
	state.value = packedState
	tracker.states[name] = state

	return nil
}

func (tracker *effectTracker) Clear(name string) {
	_ = tracker.Set(name, nil)
}

func (tracker *effectTracker) Send(target *Address, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	if target == nil {
		return errors.New("cannot send message to nil target")
	}

	packedState, err := marshall(message)
	if err != nil {
		return err
	}

	invocation := &FromFunction_Invocation{
		Target:   target,
		Argument: packedState,
	}

	tracker.invocations = append(tracker.invocations, invocation)
	return nil
}

func (tracker *effectTracker) Reply(message proto.Message) error {
	return tracker.Send(tracker.caller, message)
}

func (tracker *effectTracker) SendAfter(target *Address, duration time.Duration, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedMessage, err := marshall(message)
	if err != nil {
		return err
	}

	delayedInvocation := &FromFunction_DelayedInvocation{
		Target:    target,
		DelayInMs: duration.Milliseconds(),
		Argument:  packedMessage,
	}

	tracker.delayedInvocation = append(tracker.delayedInvocation, delayedInvocation)
	return nil
}

func (tracker *effectTracker) SendEgress(egress EgressIdentifier, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to egress")
	}

	packedMessage, err := marshall(message)
	if err != nil {
		return err
	}

	egressMessage := &FromFunction_EgressMessage{
		EgressNamespace: egress.EgressNamespace,
		EgressType:      egress.EgressType,
		Argument:        packedMessage,
	}

	tracker.outgoingEgress = append(tracker.outgoingEgress, egressMessage)
	return nil
}

func (tracker *effectTracker) fromFunction() (*FromFunction, error) {
	var mutations []*FromFunction_PersistedValueMutation
	for name, state := range tracker.states {
		if !state.updated {
			continue
		}

		var err error
		var bytes []byte
		var mutationType FromFunction_PersistedValueMutation_MutationType

		if state.value == nil {
			bytes = nil
			mutationType = FromFunction_PersistedValueMutation_DELETE
		} else {
			mutationType = FromFunction_PersistedValueMutation_MODIFY

			bytes, err = proto.Marshal(state.value)
			if err != nil {
				return nil, err
			}
		}

		mutation := &FromFunction_PersistedValueMutation{
			MutationType: mutationType,
			StateName:    name,
			StateValue:   bytes,
		}

		mutations = append(mutations, mutation)
	}

	return &FromFunction{
		Response: &FromFunction_InvocationResult{
			InvocationResult: &FromFunction_InvocationResponse{
				StateMutations:     mutations,
				OutgoingMessages:   tracker.invocations,
				DelayedInvocations: tracker.delayedInvocation,
				OutgoingEgresses:   tracker.outgoingEgress,
			},
		},
	}, nil
}
