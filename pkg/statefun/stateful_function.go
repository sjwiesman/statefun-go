package statefun

import (
	"context"
	"log"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

// A StatefulFunction is a user-defined function that can be invoked with a given input.
//This is the primitive building block for a Stateful Functions application.
//
// Concept
//
// Each individual StatefulFunction is an uniquely invokable "instance" of a registered
// StatefulFunctionSpec. Each instance is identified by an Address, representing the
// function's unique id (a string) within its type. From a user's perspective, it would seem as if
// for each unique function id, there exists a stateful instance of the function that is always
// available to be invoked within a Stateful Functions application.
//
// Invoking a StatefulFunction
//
// An individual StatefulFunction can be invoked with arbitrary input from any another
// StatefulFunction (including itself), or routed from ingresses. To invoke a
// StatefulFunction, the caller simply needs to know the Address of the target function.
//
// As a result of invoking a StatefulFunction, the function may continue to invoke other
// functions, access persisted values, or send messages to egresses.
//
// Persistent State
//
// Each individual StatefulFunction may have persistent values written to storage that is
// maintained by the system, providing consistent exactly-once and fault-tolerant guarantees. Please
// see docs in ValueSpec and AddressScopedStorage for an overview of how to
// register persistent values and access the storage
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

// The StatefulFunctionPointer type is an adapter to allow the use of
// ordinary functions as StatefulFunction's. If f is a function
// with the appropriate signature, StatefulFunctionPointer(f) is a
// StatefulFunction that calls f.
type StatefulFunctionPointer func(context.Context, AddressScopedStorage, Message) error

func (s StatefulFunctionPointer) Invoke(ctx context.Context, storage AddressScopedStorage, message Message) error {
	return s(ctx, storage, message)
}
