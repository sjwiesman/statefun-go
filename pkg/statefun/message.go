package statefun

import (
	"bytes"
	"errors"
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"time"
)

type MessageBuilder struct {
	Target    Address
	Value     interface{}
	ValueType Type
	Delay     time.Duration
}

func (m MessageBuilder) isEnvelope() {}

func (m MessageBuilder) ToMessage() (Message, error) {
	if m.Target == (Address{}) {
		return Message{}, errors.New("a message must have a non-empty target")
	}

	if m.Value == nil {
		return Message{}, errors.New("a message cannot have a nil value")
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

	buffer := bytes.Buffer{}
	err := m.ValueType.Serialize(&buffer, m.Value)
	if err != nil {
		return Message{}, err
	}

	return Message{
		target: &protocol.Address{
			Namespace: m.Target.GetNamespace(),
			Type:      m.Target.GetName(),
			Id:        m.Target.Id,
		},
		typedValue: &protocol.TypedValue{
			Typename: m.ValueType.GetTypeName().String(),
			HasValue: true,
			Value:    buffer.Bytes(),
		},
		delayMs: m.Delay.Milliseconds(),
	}, nil
}

type Message struct {
	target     *protocol.Address
	typedValue *protocol.TypedValue
	delayMs    int64
}

func (m *Message) IsBool() bool {
	return m.Is(BoolType)
}

func (m *Message) AsBool() bool {
	var receiver bool
	if err := BoolType.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}
	return receiver
}

func (m *Message) IsInt32() bool {
	return m.Is(Int32Type)
}

func (m *Message) AsInt32() int32 {
	var receiver int32
	if err := Int32Type.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}
	return receiver
}

func (m *Message) IsInt64() bool {
	return m.Is(Int64Type)
}

func (m *Message) AsInt64() int64 {
	var receiver int64
	if err := Int64Type.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}
	return receiver
}

func (m *Message) IsFloat32() bool {
	return m.Is(Float32Type)
}

func (m *Message) AsFloat32() float32 {
	var receiver float32
	if err := Float32Type.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}
	return receiver
}

func (m *Message) IsFloat64() bool {
	return m.Is(Float64Type)
}

func (m *Message) AsFloat64() float64 {
	var receiver float64
	if err := Float64Type.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}
	return receiver
}

func (m *Message) IsString() bool {
	return m.Is(StringType)
}

func (m *Message) AsString() string {
	var receiver string
	if err := StringType.Deserialize(bytes.NewReader(m.typedValue.Value), &receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize message: %w", err))
	}

	return receiver
}

func (m *Message) Is(t Type) bool {
	return t.GetTypeName().String() == m.typedValue.Typename
}

func (m *Message) As(t Type, receiver interface{}) error {
	return t.Deserialize(bytes.NewReader(m.typedValue.Value), receiver)
}
