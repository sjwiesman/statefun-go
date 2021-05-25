package statefun

import (
	"context"
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

func Self(ctx context.Context) *Address {
	return ctx.Value(selfKey).(*Address)
}

func Caller(ctx context.Context) *Address {
	return ctx.Value(callerKey).(*Address)
}

func Mailbox(ctx context.Context) chan<- Envelope {
	return ctx.Value(mailboxKey).(chan<- Envelope)
}

type StatefulFunctionSpec struct {
	FunctionType TypeName
	States       []ValueSpec
	Function     StatefulFunction
}

type StatefulFunctionPointer func(ctx context.Context, storage AddressScopedStorage, message Message) error

func (s StatefulFunctionPointer) Invoke(ctx context.Context, storage AddressScopedStorage, message Message) error {
	return s(ctx, storage, message)
}
