package statefun

import "fmt"

type FunctionType struct {
	Namespace string
	Type      string
}

func (functionType *FunctionType) String() string {
	return fmt.Sprintf("%s/%s", functionType.Namespace, functionType.Type)
}
