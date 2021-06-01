package statefun

import (
	"context"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
	"time"
)

// A Context contains information about the current function invocation, such as the invoked
// function instance's and caller's Address. It is also used for side-effects as a result of
// the invocation such as send messages to other functions or egresses, and provides access to
// AddressScopedStorage scoped to the current Address. This type is also a context.Context
// and can be used to ensure any spawned go routines do not outlive the current function
// invocation.
type Context interface {
	context.Context

	// The current invoked function instance's Address.
	Self() Address

	// The caller function instance's Address, if applicable. This is nil
	// if the message was sent to this function via an ingress.
	Caller() *Address

	// Sends out a MessageBuilder to another function.
	Send(message MessageBuilder)

	// Sends out a MessageBuilder to another function, after a specified time.Duration delay.
	SendAfter(delay time.Duration, message MessageBuilder)

	// Sends out an EgressBuilder to an egress.
	SendEgress(egress EgressBuilder)

	// The AddressScopedStorage, providing access to stored values scoped to the
	// current invoked function instance's Address (which is obtainable using Self()).
	Storage() AddressScopedStorage
}

type statefunContext struct {
	sync.Mutex
	context.Context
	self     Address
	caller   *Address
	storage  *storage
	response *protocol.FromFunction_InvocationResponse
}

func (s *statefunContext) Storage() AddressScopedStorage {
	return s.storage
}

func (s *statefunContext) Self() Address {
	return s.self
}

func (s *statefunContext) Caller() *Address {
	return s.caller
}

func (s *statefunContext) Send(message MessageBuilder) {
	s.Lock()
	defer s.Unlock()

	msg, err := message.ToMessage()

	if err != nil {
		panic(err)
	}

	invocation := &protocol.FromFunction_Invocation{
		Target:   msg.target,
		Argument: msg.typedValue,
	}

	s.response.OutgoingMessages = append(s.response.OutgoingMessages, invocation)
}

func (s *statefunContext) SendAfter(delay time.Duration, message MessageBuilder) {
	s.Lock()
	defer s.Unlock()

	msg, err := message.ToMessage()

	if err != nil {
		panic(err)
	}

	invocation := &protocol.FromFunction_DelayedInvocation{
		Target:    msg.target,
		Argument:  msg.typedValue,
		DelayInMs: delay.Milliseconds(),
	}

	s.response.DelayedInvocations = append(s.response.DelayedInvocations, invocation)
}

func (s *statefunContext) SendEgress(egress EgressBuilder) {
	s.Lock()
	defer s.Unlock()

	msg, err := egress.toEgressMessage()

	if err != nil {
		panic(err)
	}

	s.response.OutgoingEgresses = append(s.response.OutgoingEgresses, msg)
}
