package statefun

import (
	"context"
	any "google.golang.org/protobuf/types/known/anypb"
)

// StatefulFunction is a user-defined function that can be invoked with a given input.
// This is the primitive building block for a Stateful Functions application.
//
// Each individual function is a uniquely invokable "instance" of a FunctionType.
// Each function is identified by an Address, representing the function's
// unique id (a string) within its type. From a user's perspective, it would seem
// as if for each unique function id, there exists a stateful instance of the function
// that is always available to be invoked within an application.
//
// An individual function can be invoked with arbitrary input form any other
// function (including itself), or routed form an ingress via a Router. To
// executeBatch a function, the caller simply needs to know the Address of the target
// function. As a result of invoking a StatefulFunction, the function may continue
// to executeBatch other functions, modify its state, or send messages to egresses
// addressed by an egress identifier.
//
// Each individual function instance may have state that is maintained by the system,
// providing exactly-once guarantees.
type StatefulFunction interface {

	// Invoke this function with the given input.
	Invoke(ctx context.Context, runtime StatefulFunctionRuntime, msg *any.Any) error
}
