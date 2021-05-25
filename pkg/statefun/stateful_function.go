package statefun

import (
	"context"
	"statefun-sdk-go/pkg/statefun/internal"
)

type StatefulFunction interface {
	Invoke(ctx context.Context, storage AddressScopedStorage, message Message) error
}

func Self(ctx context.Context) *Address {
	return ctx.Value(internal.SelfKey).(*Address)
}

func Caller(ctx context.Context) *Address {
	return ctx.Value(internal.CallerKey).(*Address)
}

func Mailbox(ctx context.Context) chan<- Envelope {
	return ctx.Value(internal.MailboxKey).(chan<- Envelope)
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
