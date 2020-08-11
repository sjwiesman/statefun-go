package statefun

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"time"
)

// Tracks the state changes
// of the function invocations
type state struct {
	updated bool
	value   *any.Any
}

// InvocationContext is the main effect tracker of the function invocation
// It tracks all responses that will be sent back to the
// Flink runtime after the full batch has been executed.
type InvocationContext struct {
	self              *Address
	caller            *Address
	states            map[string]*state
	invocations       []*FromFunction_Invocation
	delayedInvocation []*FromFunction_DelayedInvocation
	outgoingEgress    []*FromFunction_EgressMessage
}

// Create a new context based on the target function
// and set of initial states.
func newContext(self *Address, persistedValues []*ToFunction_PersistedValue) InvocationContext {
	ctx := InvocationContext{
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

// Self returns the address of the current
// function instance under evaluation
func (ctx *InvocationContext) Self() *Address {
	return ctx.self
}

// Caller returns the address of the caller function.
// The caller may be nil if the message
// was sent directly from an ingress
func (ctx *InvocationContext) Caller() *Address {
	return ctx.caller
}

// Get returns the current value of a state accessed by name or an
// error if the state does not exist.
func (ctx *InvocationContext) Get(name string) (*any.Any, error) {
	state := ctx.states[name]
	if state == nil {
		return nil, errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	return state.value, nil
}

// GetAndUnpack retrieves the state for the given name and
// unmarshals the encoded value contained into the provided message state.
// It returns an error if the target message does not match the type
// in the Any message or if an unmarshal error occurs.
func (ctx *InvocationContext) GetAndUnpack(name string, state proto.Message) error {
	packedState := ctx.states[name]
	if packedState == nil {
		return errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	if packedState.value == nil || packedState.value.TypeUrl == "" {
		return nil
	}

	err := ptypes.UnmarshalAny(packedState.value, state)
	if err != nil {
		return err
	}

	return nil
}

// Set stores the value under the given name in state.
// It will return an error if the state does not exist.
func (ctx *InvocationContext) Set(name string, value *any.Any) error {
	state := ctx.states[name]
	if state == nil {
		return errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	state.updated = true
	state.value = value
	ctx.states[name] = state

	return nil
}

// SetAndPack stores the value under the given name in state and
// marshals the given message m into an anypb.Any message.
func (ctx *InvocationContext) SetAndPack(name string, value proto.Message) error {
	if value == nil {
		return ctx.Set(name, nil)
	}

	state := ctx.states[name]
	if state == nil {
		return errors.New(fmt.Sprintf("Unknown state name %s", name))
	}

	packedState, err := ptypes.MarshalAny(value)
	if err != nil {
		return err
	}

	state.updated = true
	state.value = packedState
	ctx.states[name] = state

	return nil
}

// Clear deletes the state registered under the name
func (ctx *InvocationContext) Clear(name string) {
	_ = ctx.Set(name, nil)
}

func (ctx *InvocationContext) Send(target *Address, message *any.Any) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	invocation := &FromFunction_Invocation{
		Target:   target,
		Argument: message,
	}

	ctx.invocations = append(ctx.invocations, invocation)
	return nil
}

func (ctx *InvocationContext) SendAndPack(target *Address, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedState, err := ptypes.MarshalAny(message)
	if err != nil {
		return err
	}

	return ctx.Send(target, packedState)
}

func (ctx *InvocationContext) Reply(message *any.Any) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	return ctx.Send(ctx.caller, message)
}

func (ctx *InvocationContext) ReplyAndPack(message proto.Message) error {
	return ctx.SendAndPack(ctx.caller, message)
}

func (ctx *InvocationContext) SendAfter(target *Address, duration time.Duration, message *any.Any) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	delayedInvocation := &FromFunction_DelayedInvocation{
		Target:    target,
		DelayInMs: duration.Milliseconds(),
		Argument:  message,
	}

	ctx.delayedInvocation = append(ctx.delayedInvocation, delayedInvocation)
	return nil
}

func (ctx *InvocationContext) SendAfterAndPack(target *Address, duration time.Duration, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to function")
	}

	packedMessage, err := ptypes.MarshalAny(message)
	if err != nil {
		return err
	}

	return ctx.SendAfter(target, duration, packedMessage)
}

func (ctx *InvocationContext) SendEgress(egress Egress, message *any.Any) error {
	if message == nil {
		return errors.New("cannot send nil message to egress")
	}

	egressMessage := &FromFunction_EgressMessage{
		EgressNamespace: egress.EgressNamespace,
		EgressType:      egress.EgressType,
		Argument:        message,
	}

	ctx.outgoingEgress = append(ctx.outgoingEgress, egressMessage)
	return nil
}

func (ctx *InvocationContext) SendEgressAndPack(egress Egress, message proto.Message) error {
	if message == nil {
		return errors.New("cannot send nil message to egress")
	}

	packedMessage, err := ptypes.MarshalAny(message)
	if err != nil {
		return err
	}

	return ctx.SendEgress(egress, packedMessage)
}

func (ctx *InvocationContext) fromFunction() (*FromFunction, error) {
	var mutations []*FromFunction_PersistedValueMutation
	for name, state := range ctx.states {
		if !state.updated {
			continue
		}

		mutationType := FromFunction_PersistedValueMutation_MODIFY
		if state.value == nil {
			mutationType = FromFunction_PersistedValueMutation_DELETE
		}

		bytes, err := proto.Marshal(state.value)
		if err != nil {
			return nil, err
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
				OutgoingMessages:   ctx.invocations,
				DelayedInvocations: ctx.delayedInvocation,
				OutgoingEgresses:   ctx.outgoingEgress,
			},
		},
	}, nil
}
