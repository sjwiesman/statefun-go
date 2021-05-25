package statefun

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoolType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := BoolType.Serialize(&buffer, true)
	assert.NoError(t, err)

	var result bool
	err = BoolType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.True(t, result)
}

func TestIntType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := Int32Type.Serialize(&buffer, int32(1))
	assert.NoError(t, err)

	var result int32
	err = Int32Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, int32(1))
}

func TestLongType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := Int64Type.Serialize(&buffer, int64(1<<45))
	assert.NoError(t, err)

	var result int64
	err = Int64Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, int64(1<<45))
}

func TestFloatType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := Float32Type.Serialize(&buffer, float32(0.5))
	assert.NoError(t, err)

	var result float32
	err = Float32Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, float32(0.5))
}

func TestDoubleType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := Float64Type.Serialize(&buffer, float64(1e-20))
	assert.NoError(t, err)

	var result float64
	err = Float64Type.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, float64(1e-20))
}

func TestStringType(t *testing.T) {
	buffer := bytes.Buffer{}
	err := StringType.Serialize(&buffer, "hello world")
	assert.NoError(t, err)

	var result string
	err = StringType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, "hello world")
}

type User struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func TestJsonType(t *testing.T) {
	buffer := bytes.Buffer{}
	userType := MakeJsonType(TypeNameFrom("org.foo.bar/UserJson"))

	err := userType.Serialize(&buffer, User{"bob", "mop"})
	assert.NoError(t, err)

	var result User
	err = userType.Deserialize(bytes.NewReader(buffer.Bytes()), &result)
	assert.NoError(t, err)

	assert.Equal(t, result, User{"bob", "mop"})
}
