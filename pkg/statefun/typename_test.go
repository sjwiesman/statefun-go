package statefun

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTypeNameParse(t *testing.T) {
	typename, err := ParseTypeName("namespace/Name")
	if err != nil {
		assert.NoError(t, err)
	}

	assert.Equal(t, typename.GetNamespace(), "namespace")
	assert.Equal(t, typename.GetName(), "Name")
}
