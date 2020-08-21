package io

import (
	"github.com/golang/protobuf/proto"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/errors"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
)

// Egress message that will be
// written out to Apache Kafka.
type KafkaRecord struct {
	// The topic to which the message
	// should be written.
	Topic string

	// An optional key to be written with
	// the message into the topic.
	Key string

	// The message to be written
	// to the topic.
	Value proto.Message
}

// Transforms a KafkaRecord into a Message that can
// be sent to an egress.
func (record *KafkaRecord) ToMessage() (proto.Message, error) {
	if record.Topic == "" {
		return nil, errors.New("cannot send a message to an empty topic")
	}

	marshalled, err := internal.Marshall(record.Value)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshall message meant for kafka topic %s", record.Topic)
	}

	var bytes []byte

	if marshalled != nil {
		bytes, err = proto.Marshal(marshalled)
		if err != nil {
			return nil, errors.Wrap(err, "failed to serialize message meant for kafka topic %s", record.Topic)
		}
	}

	return &messages.KafkaProducerRecord{
		Key:        record.Key,
		Topic:      record.Topic,
		ValueBytes: bytes,
	}, nil
}
