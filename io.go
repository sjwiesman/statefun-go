package statefun_go

import (
	"errors"
	"github.com/golang/protobuf/proto"
)

func KafkaEgressRecord(topic string, key string, message proto.Message) (*KafkaProducerRecord, error) {
	if message == nil {
		return nil, errors.New("cannot send nil message to kafka")
	}

	marshalled, err := marshall(message)
	if err != nil {
		return nil, err
	}

	bytes, err := proto.Marshal(marshalled)
	if err != nil {
		return nil, err
	}

	return &KafkaProducerRecord{
		Key:        key,
		Topic:      topic,
		ValueBytes: bytes,
	}, nil
}
