package statefun

import (
	"errors"
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"strings"
)

var (
	boolTypeName    = TypeNameFrom("io.statefun.types/bool")
	int32TypeName   = TypeNameFrom("io.statefun.types/int")
	int64TypeName   = TypeNameFrom("io.statefun.types/long")
	float32TypeName = TypeNameFrom("io.statefun.types/float")
	float64ypeName  = TypeNameFrom("io.statefun.types/double")
	stringTypeName  = TypeNameFrom("io.statefun.types/string")
)

// A TypeName is used to uniquely identify objects within
// a Stateful Functions application, including functions,
// egresses, and types. TypeName's serve as an integral
// part of identifying these objects for message delivery
// as well as message data serialization and deserialization.
type TypeName interface {
	fmt.Stringer
	GetNamespace() string
	GetName() string
}

type typeName struct {
	namespace      string
	name           string
	typenameString string
}

func (t typeName) String() string {
	return t.typenameString
}

func (t typeName) GetNamespace() string {
	return t.namespace
}

func (t typeName) GetName() string {
	return t.name
}

// Creates a TypeName from a canonical string
// in the format `<namespace>/<Name>`. This Function
// assumes correctly formatted strings and will panic
// on error. For runtime error handling please
// see ParseTypeName.
func TypeNameFrom(typename string) TypeName {
	result, err := ParseTypeName(typename)
	if err != nil {
		panic(err)
	}

	return result
}

// Creates a TypeName from a canonical string
// in the format `<namespace>/<Name>`.
func ParseTypeName(typename string) (TypeName, error) {
	position := strings.LastIndex(typename, "/")
	if position <= 0 || position == len(typename)-1 {
		return nil, fmt.Errorf("%v does not conform to the <namespace>/<Name> format", typename)
	}

	namespace := typename[:position]
	name := typename[position+1:]

	if namespace[len(namespace)-1] == '/' {
		namespace = namespace[:len(namespace)-1]
	}

	if len(namespace) == 0 {
		return nil, errors.New("namespace cannot be empty")
	}

	if len(name) == 0 {
		return nil, errors.New("Name cannot be empty")
	}

	return typeName{
		namespace, name, typename,
	}, nil
}

type Address struct {
	TypeName
	Id string
}

func addressFromInternal(a *protocol.Address) *Address {
	return &Address{
		TypeName: typeNameFromParts(a.Namespace, a.Type),
		Id:       a.Id,
	}
}

func typeNameFromParts(namespace, name string) typeName {
	return typeName{
		namespace:      namespace,
		name:           name,
		typenameString: fmt.Sprintf("%s/%s", namespace, name),
	}
}
