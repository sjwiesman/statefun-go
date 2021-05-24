package statefun

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBasicIntMessage(t *testing.T) {
	typename, err := ParseTypeName("foo/bar")
	assert.NoError(t, err)

	message, err := MessageBuilder{
		Value: int32(1),
	}.ToMessage(Address{
		TypeName: typename,
		Id:       "a",
	})
	assert.NoError(t, err)
	assert.True(t, message.IsInt32())

	value, err := message.AsInt32()
	assert.NoError(t, err)
	assert.Equal(t, value, int32(1))
}

func TestMessageWithType(t *testing.T) {
	typename, err := ParseTypeName("foo/bar")
	assert.NoError(t, err)

	message, err := MessageBuilder{
		Value:     float32(5.0),
		ValueType: Float32Type,
	}.ToMessage(Address{
		TypeName: typename,
		Id:       "a",
	})
	assert.NoError(t, err)
	assert.True(t, message.IsFloat32())

	value, err := message.AsFloat32()
	assert.NoError(t, err)
	assert.Equal(t, value, float32(5.0))
}
