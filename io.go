package statefun_go

import (
	"github.com/golang/protobuf/proto"
)

func KafkaEgressRecord(topic string, key string, message proto.Message) (*KafkaProducerRecord, error) {
	marshalled, err := marshall(message)
	if err != nil {
		return nil, err
	}

	var bytes []byte

	if marshalled != nil {
		bytes, err = proto.Marshal(marshalled)
		if err != nil {
			return nil, err
		}
	}

	return &KafkaProducerRecord{
		Key:        key,
		Topic:      topic,
		ValueBytes: bytes,
	}, nil
}
