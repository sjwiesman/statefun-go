package statefun

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/proto"
	"io"
	"io/ioutil"
	"log"
	"strings"
)

// This interface is the core abstraction used byt Stateful
// Function's type system, and consists of a few things
// that StateFun uses to handle Message's and ValueSpec's
//
// 1. TypeName to identify the type.
// 2. (De)serialization methods for marshalling and unmarshalling data
//
// Cross-language primitive types
//
// StateFun's type system has cross-language support for common primitive
// types, such as boolean, integer (int32), long (int64), etc. These
// primitive types have built-in Type's implemented for them already
// with predefined TypeName's.
//
// These primitives have standard encoding across all StateFun language
// SDKs, so functions in various other languages (Java, Python, etc) can
// message Golang functions by directly sending supported primitive
// values as message arguments. Moreover, the type system is used for
// state values as well; so you can expect that a function can safely
// read previous state after reimplementing it in a different language.
//
// Common custom types
//
// The type system is also very easily extensible to support more complex types.
// The Go SDK ships with predefined support for JSON and Protobuf - see MakeJsonType
// MakeProtobufType. For other formats, it is just a matter of implementing
// your own Type with a custom typename and serializer.
type Type interface {
	GetTypeName() TypeName

	Deserialize(r io.Reader, receiver interface{}) error

	Serialize(writer io.Writer, data interface{}) error
}

type PrimitiveType int

const (
	BoolType PrimitiveType = iota
	Int32Type
	Int64Type
	Float32Type
	Float64Type
	StringType
)

func (p PrimitiveType) GetTypeName() TypeName {
	switch p {
	case BoolType:
		return boolTypeName
	case Int32Type:
		return int32TypeName
	case Int64Type:
		return int64TypeName
	case Float32Type:
		return float32TypeName
	case Float64Type:
		return float64ypeName
	case StringType:
		return stringTypeName
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil
	}
}

func (p PrimitiveType) Deserialize(r io.Reader, receiver interface{}) error {
	switch p {
	case BoolType:
		switch receiver.(type) {
		case *bool:
			return binary.Read(r, binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type bool or *bool")
		}
	case Int32Type:
		switch receiver.(type) {
		case *int32:
			return binary.Read(r, binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *int32")
		}
	case Int64Type:
		switch receiver.(type) {
		case *int64:
			return binary.Read(r, binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *int64")
		}
	case Float32Type:
		switch receiver.(type) {
		case *float32:
			return binary.Read(r, binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *float32")
		}
	case Float64Type:
		switch receiver.(type) {
		case *float64:
			return binary.Read(r, binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *float64")
		}
	case StringType:
		switch receiver := receiver.(type) {
		case *string:
			buf := strings.Builder{}
			_, _ = io.Copy(&buf, r)
			*receiver = buf.String()
			return nil
		default:
			return errors.New("receiver must be of type *string")
		}
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil
	}
}

func (p PrimitiveType) Serialize(writer io.Writer, data interface{}) error {
	switch p {
	case BoolType:
		switch data.(type) {
		case bool, *bool:
			return binary.Write(writer, binary.BigEndian, data)
		default:
			return errors.New("data must be of type bool or *bool")
		}
	case Int32Type:
		switch data.(type) {
		case int32, *int32:
			return binary.Write(writer, binary.BigEndian, data)
		default:
			return errors.New("data must be of type int32 or *int32")
		}
	case Int64Type:
		switch data.(type) {
		case int64, *int64:
			return binary.Write(writer, binary.BigEndian, data)
		default:
			return errors.New("data must be of type int64 or *int64")
		}
	case Float32Type:
		switch data.(type) {
		case float32, *float32:
			return binary.Write(writer, binary.BigEndian, data)
		default:
			return errors.New("data must be of type float32 or *float32")
		}
	case Float64Type:
		switch data.(type) {
		case float64, *float64:
			return binary.Write(writer, binary.BigEndian, data)
		default:
			return errors.New("data must be of type float64 or *float64")
		}
	case StringType:
		switch data := data.(type) {
		case string:
			switch writer := writer.(type) {
			case io.StringWriter:
				_, err := writer.WriteString(data)
				return err
			default:
				_, err := writer.Write([]byte(data))
				return err
			}
		case *string:
			switch writer := writer.(type) {
			case io.StringWriter:
				_, err := writer.WriteString(*data)
				return err
			default:
				_, err := writer.Write([]byte(*data))
				return err
			}
		default:
			return errors.New("data must be of type string or *string")
		}
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil
	}
}

type jsonType struct {
	typeName TypeName
}

// Creates a new Type with a given TypeName
// using the standard Go JSON library.
func MakeJsonType(name TypeName) Type {
	return jsonType{typeName: name}
}

func (j jsonType) GetTypeName() TypeName {
	return j.typeName
}

func (j jsonType) Deserialize(r io.Reader, receiver interface{}) error {
	return json.NewDecoder(r).Decode(receiver)
}

func (j jsonType) Serialize(writer io.Writer, data interface{}) error {
	return json.NewEncoder(writer).Encode(data)
}

type protoType struct {
	typeName TypeName
}

// Creates a new Type for the given protobuf Message.
func MakeProtobufType(m proto.Message) Type {
	return MakeProtobufTypeWithNamespace(m, "type.googleapis.com")
}

// Creates a new Type for the given protobuf Message with a custom namespace.
func MakeProtobufTypeWithNamespace(m proto.Message, namespace string) Type {
	name := proto.MessageName(m)
	tName, _ := TypeNameFromParts(namespace, string(name))
	return protoType{
		typeName: tName,
	}
}

func (p protoType) GetTypeName() TypeName {
	return p.typeName
}

func (p protoType) Deserialize(r io.Reader, receiver interface{}) error {
	switch receiver := receiver.(type) {
	case proto.Message:
		data, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		return proto.Unmarshal(data, receiver)
	default:
		return errors.New("receiver must implement proto.Message")
	}
}

func (p protoType) Serialize(writer io.Writer, data interface{}) error {
	switch data := data.(type) {
	case proto.Message:
		if value, err := proto.Marshal(data); err != nil {
			return err
		} else {
			_, err = writer.Write(value)
			return err
		}

	default:
		return errors.New("data must implement proto.Message")
	}
}
