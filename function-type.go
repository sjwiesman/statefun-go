package statefun_go

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
