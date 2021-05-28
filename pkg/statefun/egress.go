package statefun

import (
	"bytes"
	"encoding/binary"
	"errors"
	"google.golang.org/protobuf/proto"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type EgressBuilder interface {
	Envelope

	toEgressMessage() (*protocol.FromFunction_EgressMessage, error)
}

// Builds a message that can be emitted to a Kafka generic egress.
// If a ValueType is provided, then Value will be serialized according to the
// provided ValueType's serializer. Otherwise we will try to convert Value to bytes
// if it is one of:
//   - utf-8 string
//   - []bytes
//   - an int (as defined by Kafka's serialization format)
//   - float (as defined by Kafka's serialization format)
type KafkaEgressBuilder struct {
	// The TypeName as specified in module.yaml
	Target TypeName

	// The Kafka destination topic for that record
	Topic string

	// The utf8 encoded string key to produce (can be empty)
	Key string

	// The value to produce
	Value interface{}

	// An optional hint to this value type
	ValueType Type
}

func (k KafkaEgressBuilder) isEnvelope() {}

func (k KafkaEgressBuilder) toEgressMessage() (*protocol.FromFunction_EgressMessage, error) {
	if k.Target == nil {
		return nil, errors.New("an egress record requires a Target")
	}
	if k.Topic == "" {
		return nil, errors.New("A Kafka record requires a topic")
	}

	if k.Value == nil {
		return nil, errors.New("A Kafka record requires a value")
	}

	buffer := bytes.Buffer{}
	if k.ValueType != nil {
		if err := k.ValueType.Serialize(&buffer, k.Value); err != nil {
			return nil, err
		}
	} else {
		switch value := k.Value.(type) {
		case string:
			_ = StringType.Serialize(&buffer, value)
		case []byte:
			buffer.Write(value)
		case int, int32, int64, float32, float64:
			if err := binary.Write(&buffer, binary.BigEndian, value); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New("unable to convert value to bytes")
		}
	}

	kafka := protocol.KafkaProducerRecord{
		Key:        k.Key,
		ValueBytes: buffer.Bytes(),
		Topic:      k.Topic,
	}

	value, err := proto.Marshal(&kafka)
	if err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: k.Target.GetNamespace(),
		EgressType:      k.Target.GetType(),
		Argument: &protocol.TypedValue{
			Typename: "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord",
			HasValue: true,
			Value:    value,
		},
	}, nil
}

// Builds a message that can be emitted to a Kinesis generic egress.
// If a ValueType is provided, then Value will be serialized according to the
// provided ValueType's serializer. Otherwise we will try to convert Value to bytes
// if it is one of:
//   - utf-8 string
//   - []byte
type KinesisEgressBuilder struct {
	// The TypeName as specified in module.yaml
	Target TypeName

	// The Kinesis destination stream for that record
	Stream string

	// The value to produce
	Value interface{}

	// An optional hint to this value type
	ValueType Type

	// The utf8 encoded string partition key to use
	PartitionKey string

	// A utf8 encoded string explicit hash key to use (can be empty)
	ExplicitHashKey string
}

func (k KinesisEgressBuilder) isEnvelope() {}

func (k KinesisEgressBuilder) toEgressMessage() (*protocol.FromFunction_EgressMessage, error) {
	if k.Target == nil {
		return nil, errors.New("an egress record requires a Target")
	} else if k.Stream == "" {
		return nil, errors.New("missing destination Kinesis stream")
	} else if k.Value == nil {
		return nil, errors.New("missing value")
	} else if k.PartitionKey == "" {
		return nil, errors.New("missing partition key")
	}

	buffer := bytes.Buffer{}
	if k.ValueType != nil {
		if err := k.ValueType.Serialize(&buffer, k.Value); err != nil {
			return nil, err
		}
	} else {
		switch value := k.Value.(type) {
		case string:
			_ = StringType.Serialize(&buffer, value)
		case []byte:
			buffer.Write(value)
		default:
			return nil, errors.New("unable to convert value to bytes")
		}
	}

	kinesis := protocol.KinesisEgressRecord{
		PartitionKey:    k.PartitionKey,
		ValueBytes:      buffer.Bytes(),
		Stream:          k.Stream,
		ExplicitHashKey: k.ExplicitHashKey,
	}

	value, err := proto.Marshal(&kinesis)
	if err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: k.Target.GetNamespace(),
		EgressType:      k.Target.GetType(),
		Argument: &protocol.TypedValue{
			Typename: "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord",
			HasValue: true,
			Value:    value,
		},
	}, nil
}

// Create a generic egress record. For Kafka
// and Kinesis see KafkaEgressBuilder and
// KinesisEgressBuilder respectively
type GenericEgressBuilder struct {
	// The TypeName as specified when registered
	Target TypeName

	// The value to produce
	Value interface{}

	// The values type
	ValueType Type
}

func (g GenericEgressBuilder) isEnvelope() {}

func (g GenericEgressBuilder) toEgressMessage() (*protocol.FromFunction_EgressMessage, error) {
	if g.Target == nil {
		return nil, errors.New("an egress record requires a Target")
	} else if g.ValueType == nil {
		return nil, errors.New("missing value type")
	} else if g.Value == nil {
		return nil, errors.New("missing value")
	}

	buffer := bytes.Buffer{}
	if err := g.ValueType.Serialize(&buffer, g.Value); err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: g.Target.GetNamespace(),
		EgressType:      g.Target.GetType(),
		Argument: &protocol.TypedValue{
			Typename: g.ValueType.GetTypeName().String(),
			HasValue: true,
			Value:    buffer.Bytes(),
		},
	}, nil
}
