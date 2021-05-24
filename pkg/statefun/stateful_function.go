package statefun

import (
	"context"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"time"
)

type Mailbox struct {
	outgoingMessages   []*protocol.FromFunction_Invocation
	delayedInvocations []*protocol.FromFunction_DelayedInvocation
	outgoingEgresses   []*protocol.FromFunction_EgressMessage
}

func (m *Mailbox) append(other *Mailbox) {
	m.outgoingMessages = append(m.outgoingMessages, other.outgoingMessages...)
	m.delayedInvocations = append(m.delayedInvocations, other.delayedInvocations...)
	m.outgoingEgresses = append(m.outgoingEgresses, other.outgoingEgresses...)
}

func (m *Mailbox) Send(message Message) {
	invocation := &protocol.FromFunction_Invocation{
		Target:   message.target,
		Argument: message.typedValue,
	}

	m.outgoingMessages = append(m.outgoingMessages, invocation)
}

func (m *Mailbox) SendAfter(duration time.Duration, message Message) {
	invocation := &protocol.FromFunction_DelayedInvocation{
		Target:    message.target,
		DelayInMs: duration.Milliseconds(),
		Argument:  message.typedValue,
	}

	m.delayedInvocations = append(m.delayedInvocations, invocation)
}

func (m *Mailbox) SendEgress(message EgressMessage) {
	m.outgoingEgresses = append(m.outgoingEgresses, message.internal)
}

type StatefulFunction interface {
	Invoke(ctx context.Context, storage *AddressScopedStorage, message Message) (*Mailbox, error)
}

type contextKey string

const (
	selfKey   = contextKey("self")
	callerKey = contextKey("caller")
)

func Self(ctx context.Context) *Address {
	return ctx.Value(selfKey).(*Address)
}

func Caller(ctx context.Context) *Address {
	return ctx.Value(callerKey).(*Address)
}

type StatefulFunctionSpec struct {
	Id       TypeName
	States   []ValueSpec
	Function StatefulFunction
}

type StatefulFunctionPointer func(ctx context.Context, storage *AddressScopedStorage, message Message) (*Mailbox, error)

func (s StatefulFunctionPointer) Invoke(ctx context.Context, storage *AddressScopedStorage, message Message) (*Mailbox, error) {
	return s(ctx, storage, message)
}
