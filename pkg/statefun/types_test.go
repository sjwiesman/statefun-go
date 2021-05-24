package statefun

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoolType(t *testing.T) {
	out, err := BoolType.Serialize(true)
	assert.NoError(t, err)

	var result bool
	err = BoolType.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.True(t, result)
}

func TestIntType(t *testing.T) {
	out, err := Int32Type.Serialize(int32(1))
	assert.NoError(t, err)

	var result int32
	err = Int32Type.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, int32(1))
}

func TestLongType(t *testing.T) {
	out, err := Int64Type.Serialize(int64(1 << 45))
	assert.NoError(t, err)

	var result int64
	err = Int64Type.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, int64(1<<45))
}

func TestFloatType(t *testing.T) {
	out, err := Float32Type.Serialize(float32(0.5))
	assert.NoError(t, err)

	var result float32
	err = Float32Type.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, float32(0.5))
}

func TestDoubleType(t *testing.T) {
	out, err := Float64Type.Serialize(float64(1e-20))
	assert.NoError(t, err)

	var result float64
	err = Float64Type.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, float64(1e-20))
}

func TestStringType(t *testing.T) {
	out, err := StringType.Serialize("hello world")
	assert.NoError(t, err)

	var result string
	err = StringType.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, "hello world")
}

type User struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func TestJsonType(t *testing.T) {
	userType := MakeJsonType(TypeNameFrom("org.foo.bar/UserJson"))

	out, err := userType.Serialize(User{"bob", "mop"})
	assert.NoError(t, err)

	var result User
	err = userType.Deserialize(&result, out)
	assert.NoError(t, err)

	assert.Equal(t, result, User{"bob", "mop"})
}
