package io

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKafkaRecord_ToMessage(t *testing.T) {
	record := KafkaRecord{}
	_, err := record.ToMessage()

	assert.Error(t, err, "kafka record should enforce non-empty topic")
}

func TestKinesisRecord_ToMessage(t *testing.T) {
	record := KinesisRecord{}
	_, err := record.ToMessage()

	assert.Error(t, err, "kinesis record should enforce non-empty stream")
}
