package statefun

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTypeNameParse(t *testing.T) {
	typename, err := ParseTypeName("namespace/name")

	assert.NoError(t, err)
	assert.Equal(t, typename.GetNamespace(), "namespace")
	assert.Equal(t, typename.GetName(), "name")
}

func TestNoNamespace(t *testing.T) {
	_, err := ParseTypeName("/bar")
	assert.Error(t, err)
}

func TestNoName(t *testing.T) {
	_, err := ParseTypeName("n/")
	assert.Error(t, err)
}

func TestNoNamespaceOrName(t *testing.T) {
	_, err := ParseTypeName("/")
	assert.Error(t, err)
}

func TestEmptyString(t *testing.T) {
	_, err := ParseTypeName("")
	assert.Error(t, err)
}
