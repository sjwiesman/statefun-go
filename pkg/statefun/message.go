package statefun

import (
	"errors"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type MessageBuilder struct {
	Value     interface{}
	ValueType Type
}

func (m MessageBuilder) ToMessage(target Address) (Message, error) {
	if m.Value == nil {
		return Message{}, errors.New("A message cannot have a nil value")
	}

	if m.ValueType == nil {
		switch m.Value.(type) {
		case int:
			return Message{}, errors.New("ambiguous integer type; please specify int32 or int64")
		case bool, *bool:
			m.ValueType = BoolType
		case int32, *int32:
			m.ValueType = Int32Type
		case int64, *int64:
			m.ValueType = Int64Type
		case float32, *float32:
			m.ValueType = Float32Type
		case float64, *float64:
			m.ValueType = Float64Type
		case string, *string:
			m.ValueType = StringType
		default:
			return Message{}, errors.New("message contains non-primitive type, please supply a non-nil Type")
		}
	}

	data, err := m.ValueType.Serialize(m.Value)
	if err != nil {
		return Message{}, err
	}

	return Message{
		target: &protocol.Address{
			Namespace: target.GetNamespace(),
			Type:      target.GetName(),
			Id:        target.Id,
		},
		typedValue: &protocol.TypedValue{
			Typename: m.ValueType.GetTypeName().String(),
			HasValue: true,
			Value:    data,
		},
	}, nil
}

type Message struct {
	target     *protocol.Address
	typedValue *protocol.TypedValue
}

func (m *Message) IsBool() bool {
	return m.Is(BoolType)
}

func (m *Message) AsBool() (bool, error) {
	var receiver bool
	err := BoolType.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) IsInt32() bool {
	return m.Is(Int32Type)
}

func (m *Message) AsInt32() (int32, error) {
	var receiver int32
	err := Int32Type.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) IsInt64() bool {
	return m.Is(Int64Type)
}

func (m *Message) AsInt64() (int64, error) {
	var receiver int64
	err := Int64Type.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) IsFloat32() bool {
	return m.Is(Float32Type)
}

func (m *Message) AsFloat32() (float32, error) {
	var receiver float32
	err := Float32Type.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) IsFloat64() bool {
	return m.Is(Float64Type)
}

func (m *Message) AsFloat64() (float64, error) {
	var receiver float64
	err := Float64Type.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) IsString() bool {
	return m.Is(StringType)
}

func (m *Message) AsString() (string, error) {
	var receiver string
	err := StringType.Deserialize(&receiver, m.typedValue.Value)
	return receiver, err
}

func (m *Message) Is(t Type) bool {
	return t.GetTypeName().String() == m.typedValue.Typename
}

func (m *Message) As(t Type, receiver interface{}) error {
	return t.Deserialize(receiver, m.typedValue.Value)
}
