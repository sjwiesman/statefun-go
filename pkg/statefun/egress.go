package statefun

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/protobuf/proto"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type EgressMessage struct {
	internal *protocol.FromFunction_EgressMessage
}

type EgressBuilder interface {
	ToEgressMessage(target TypeName) (EgressMessage, error)
}

type KafkaEgressBuilder struct {
	Topic     string
	Key       string
	Value     interface{}
	ValueType Type
}

func (k KafkaEgressBuilder) ToEgressMessage(target TypeName) (EgressMessage, error) {
	if k.Topic == "" {
		return EgressMessage{}, errors.New("A Kafka record requires a topic")
	}

	if k.Value == nil {
		return EgressMessage{}, errors.New("A Kafka record requires a value")
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
		case int32, int64, float32, float64:
			buffer := bytes.Buffer{}
			err = binary.Write(&buffer, binary.BigEndian, data)
			data = buffer.Bytes()
		default:
			err = errors.New("unable to convert value to bytes")
		}
	}

	if err != nil {
		return EgressMessage{}, err
	}

	kafka := protocol.KafkaProducerRecord{
		Key:        k.Key,
		ValueBytes: data,
		Topic:      k.Topic,
	}

	value, err := proto.Marshal(&kafka)
	if err != nil {
		return EgressMessage{}, err
	}

	m := EgressMessage{
		internal: &protocol.FromFunction_EgressMessage{
			EgressNamespace: target.GetNamespace(),
			EgressType:      target.GetName(),
			Argument: &protocol.TypedValue{
				Typename: "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord",
				HasValue: true,
				Value:    value,
			},
		},
	}

	return m, nil
}

type KinesisEgressBuilder struct {
	Stream          string
	Value           interface{}
	ValueType       Type
	PartitionKey    string
	ExplicitHashKey string
}

func (k KinesisEgressBuilder) ToEgressMessage(target Address) (EgressMessage, error) {
	if k.Stream == "" {
		return EgressMessage{}, errors.New("missing destination Kinesis stream")
	} else if k.Value == nil {
		return EgressMessage{}, errors.New("missing value")
	} else if k.PartitionKey == "" {
		return EgressMessage{}, errors.New("missing partition key")
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
		return EgressMessage{}, err
	}

	kinesis := protocol.KinesisEgressRecord{
		PartitionKey:    k.PartitionKey,
		ValueBytes:      data,
		Stream:          k.Stream,
		ExplicitHashKey: k.ExplicitHashKey,
	}

	value, err := proto.Marshal(&kinesis)
	if err != nil {
		return EgressMessage{}, err
	}

	m := EgressMessage{
		internal: &protocol.FromFunction_EgressMessage{
			EgressNamespace: target.GetNamespace(),
			EgressType:      target.GetName(),
			Argument: &protocol.TypedValue{
				Typename: "type.googleapis.com/io.statefun.sdk.egress.KinesisEgressRecord",
				HasValue: true,
				Value:    value,
			},
		},
	}

	return m, nil
}
