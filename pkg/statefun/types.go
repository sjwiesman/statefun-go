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

func MakeProtobufType(m proto.Message) Type {
	return MakeProtobufTypeWithNamespace(m, "type.googleapis.com")
}

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
