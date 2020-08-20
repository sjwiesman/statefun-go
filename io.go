package statefun_go

import (
	"github.com/golang/protobuf/proto"
	"statefun-go/internal"
)

// Adds metadata to a record that is
// to be written out to Kakfa.
func KafkaEgressRecord(topic string, key string, message proto.Message) (proto.Message, error) {
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

	return &internal.KafkaProducerRecord{
		Key:        key,
		Topic:      topic,
		ValueBytes: bytes,
	}, nil
}
