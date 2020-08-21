package statefun

import "context"

type contextKey string

const (
	selfKey   = contextKey("self")
	callerKey = contextKey("caller")
)

// Self returns the address of the current
// function instance under evaluation
func Self(ctx context.Context) *Address {
	return ctx.Value(selfKey).(*Address)
}

// Caller returns the address of the caller function.
// The caller may be nil if the message
// was sent directly from an ingress
func Caller(ctx context.Context) *Address {
	return ctx.Value(callerKey).(*Address)
}
