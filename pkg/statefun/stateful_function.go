package statefun

import (
	"context"
	"log"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type StatefulFunction interface {
	Invoke(ctx context.Context, storage AddressScopedStorage, message Message) error
}

type contextKey string

const (
	selfKey    = contextKey("self")
	callerKey  = contextKey("caller")
	mailboxKey = contextKey("mailbox")
)

func setSelf(ctx context.Context, address *protocol.Address) context.Context {
	if address == nil {
		log.Fatal("function address cannot be nil")
	}

	return context.WithValue(ctx, selfKey, addressFromInternal(address))
}

func setCaller(ctx context.Context, address *protocol.Address) context.Context {
	if address == nil {
		return ctx
	}

	return context.WithValue(ctx, callerKey, addressFromInternal(address))
}

func setMailbox(ctx context.Context, mailbox chan<- Envelope) context.Context {
	return context.WithValue(ctx, mailboxKey, mailbox)
}

// Returns the current invoked function instance's Address
func Self(ctx context.Context) *Address {
	return ctx.Value(selfKey).(*Address)
}

// Returns the call functions instance's Address, if applicable.
// This function will return nil if the message was sent to
// this function via an ingress.
func Caller(ctx context.Context) *Address {
	return ctx.Value(callerKey).(*Address)
}

// Returns this functions mailbox channel. This can
// be used to send messages to other functions and
// egresses.
func Mailbox(ctx context.Context) chan<- Envelope {
	return ctx.Value(mailboxKey).(chan<- Envelope)
}

// Specification for a Stateful Function, identifiable
// by a unique TypeName.
type StatefulFunctionSpec struct {
	// The unique TypeName associated
	// the the StatefulFunction being defined.
	FunctionType TypeName

	// A slice of registered ValueSpec's that will be used
	// by this function. A function may only access values
	// that have been eagerly registered as part of its spec.
	States []ValueSpec

	// The physical StatefulFunction instance.
	Function StatefulFunction
}

type StatefulFunctionPointer func(ctx context.Context, storage AddressScopedStorage, message Message) error

func (s StatefulFunctionPointer) Invoke(ctx context.Context, storage AddressScopedStorage, message Message) error {
	return s(ctx, storage, message)
}
