package statefun

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
)

func KafkaEgressRecord(topic string, key string, message *any.Any) (*KafkaProducerRecord, error) {
	if message == nil {
		return nil, errors.New("cannot send nil message to kafka")
	}

	valueBytes, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}

	return &KafkaProducerRecord{
		Key:        key,
		Topic:      topic,
		ValueBytes: valueBytes,
	}, nil
}

func KafkaEgressRecordPack(topic string, key string, message proto.Message) (*KafkaProducerRecord, error) {
	if message == nil {
		return nil, errors.New("cannot send nil message to kafka")
	}

	packedMessage, err := ptypes.MarshalAny(message)
	if err != nil {
		return nil, err
	}

	return KafkaEgressRecord(topic, key, packedMessage)
}
