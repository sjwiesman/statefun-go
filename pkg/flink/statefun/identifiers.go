package statefun

import "fmt"

// A reference to a stateful function, consisting of a namespace and a name.
// A function's type is part of a function's Address and serves as integral
// part of an individual function's identity.
type FunctionType struct {
	Namespace string
	Type      string
}

func (functionType *FunctionType) String() string {
	return fmt.Sprintf("%s/%s", functionType.Namespace, functionType.Type)
}

// An Address is the unique identity of an individual {@link StatefulFunction}, containing
// of the function's FunctionType and an unique identifier within the type. The function's
// type denotes the class of function to invoke, while the unique identifier addresses the
// invocation to a specific function instance.
type Address struct {
	FunctionType FunctionType
	Id           string
}

func (address *Address) String() string {
	return fmt.Sprintf("%s/%s", address.FunctionType.String(), address.Id)
}
