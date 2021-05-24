package statefun

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
)

type Type interface {
	GetTypeName() TypeName

	Deserialize(receiver interface{}, data []byte) error

	Serialize(data interface{}) ([]byte, error)
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

func (p PrimitiveType) Deserialize(receiver interface{}, data []byte) error {
	switch p {
	case BoolType:
		switch receiver.(type) {
		case *bool:
			return binary.Read(bytes.NewReader(data), binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type bool or *bool")
		}
	case Int32Type:
		switch receiver.(type) {
		case *int32:
			return binary.Read(bytes.NewReader(data), binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *int32")
		}
	case Int64Type:
		switch receiver.(type) {
		case *int64:
			return binary.Read(bytes.NewReader(data), binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *int64")
		}
	case Float32Type:
		switch receiver.(type) {
		case *float32:
			return binary.Read(bytes.NewReader(data), binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *float32")
		}
	case Float64Type:
		switch receiver.(type) {
		case *float64:
			return binary.Read(bytes.NewReader(data), binary.BigEndian, receiver)
		default:
			return errors.New("receiver must be of type *float64")
		}
	case StringType:
		switch receiver := receiver.(type) {
		case *string:
			*receiver = string(data)
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

func (p PrimitiveType) Serialize(data interface{}) ([]byte, error) {
	switch p {
	case BoolType:
		switch data.(type) {
		case bool, *bool:
			buffer := bytes.Buffer{}
			if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
				return nil, err
			}

			return buffer.Bytes(), nil
		default:
			return nil, errors.New("data must be of type bool or *bool")
		}
	case Int32Type:
		switch data.(type) {
		case int32, *int32:
			buffer := bytes.Buffer{}
			if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
				return nil, err
			}

			return buffer.Bytes(), nil
		default:
			return nil, errors.New("data must be of type int32 or *int32")
		}
	case Int64Type:
		switch data.(type) {
		case int64, *int64:
			buffer := bytes.Buffer{}
			if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
				return nil, err
			}

			return buffer.Bytes(), nil
		default:
			return nil, errors.New("data must be of type int64 or *int64")
		}
	case Float32Type:
		switch data.(type) {
		case float32, *float32:
			buffer := bytes.Buffer{}
			if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
				return nil, err
			}

			return buffer.Bytes(), nil
		default:
			return nil, errors.New("data must be of type float32 or *float32")
		}
	case Float64Type:
		switch data.(type) {
		case float64, *float64:
			buffer := bytes.Buffer{}
			if err := binary.Write(&buffer, binary.BigEndian, data); err != nil {
				return nil, err
			}

			return buffer.Bytes(), nil
		default:
			return nil, errors.New("data must be of type float64 or *float64")
		}
	case StringType:
		switch data := data.(type) {
		case string:
			return []byte(data), nil
		case *string:
			return []byte(*data), nil
		default:
			return nil, errors.New("data must be of type string or *string")
		}
	default:
		log.Fatalf("unknown primitive type %v", p)
		// unreachable
		return nil, nil
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

func (j jsonType) Deserialize(receiver interface{}, data []byte) error {
	return json.Unmarshal(data, receiver)
}

func (j jsonType) Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}
