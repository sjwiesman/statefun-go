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

type KafkaEgressBuilder struct {
	Target    TypeName
	Topic     string
	Key       string
	Value     interface{}
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

	var data []byte
	var err error
	if k.ValueType != nil {
		data, err = k.ValueType.Serialize(k.Value)

	} else {
		switch value := k.Value.(type) {
		case string:
			data, _ = StringType.Serialize(value)
		case []byte:
			data = value
		case int, int32, int64, float32, float64:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.BigEndian, data)
			data = buffer.Bytes()
		default:
			err = errors.New("unable to convert value to bytes")
		}
	}

	if err != nil {
		return nil, err
	}

	kafka := protocol.KafkaProducerRecord{
		Key:        k.Key,
		ValueBytes: data,
		Topic:      k.Topic,
	}

	value, err := proto.Marshal(&kafka)
	if err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: k.Target.GetNamespace(),
		EgressType:      k.Target.GetName(),
		Argument: &protocol.TypedValue{
			Typename: "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord",
			HasValue: true,
			Value:    value,
		},
	}, nil
}

type KinesisEgressBuilder struct {
	Target          TypeName
	Stream          string
	Value           interface{}
	ValueType       Type
	PartitionKey    string
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

	var data []byte
	var err error
	if k.ValueType != nil {
		data, err = k.ValueType.Serialize(k.Value)

	} else {
		switch value := k.Value.(type) {
		case string:
			data, _ = StringType.Serialize(value)
		case []byte:
			data = value
		default:
			err = errors.New("unable to convert value to bytes")
		}
	}

	if err != nil {
		return nil, err
	}

	kinesis := protocol.KinesisEgressRecord{
		PartitionKey:    k.PartitionKey,
		ValueBytes:      data,
		Stream:          k.Stream,
		ExplicitHashKey: k.ExplicitHashKey,
	}

	value, err := proto.Marshal(&kinesis)
	if err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: k.Target.GetNamespace(),
		EgressType:      k.Target.GetName(),
		Argument: &protocol.TypedValue{
			Typename: "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord",
			HasValue: true,
			Value:    value,
		},
	}, nil
}

type GenericEgressBuilder struct {
	Target    TypeName
	Value     interface{}
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

	data, err := g.ValueType.Serialize(g.Value)
	if err != nil {
		return nil, err
	}

	return &protocol.FromFunction_EgressMessage{
		EgressNamespace: g.Target.GetNamespace(),
		EgressType:      g.Target.GetName(),
		Argument: &protocol.TypedValue{
			Typename: g.ValueType.GetTypeName().String(),
			HasValue: true,
			Value:    data,
		},
	}, nil
}
