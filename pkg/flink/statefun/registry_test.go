package statefun

import (
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitialRegistration(t *testing.T) {

	var providedStates []*messages.ToFunction_PersistedValue

	statesSpecs := map[string]*messages.FromFunction_PersistedValueSpec{
		"A": {
			StateName: "A",
		},
	}

	results := checkRegisteredStates(providedStates, statesSpecs)
	assert.NotNil(t, results, "missing states should not be nil")
	assert.Equal(t, 1, len(results.GetIncompleteInvocationContext().MissingValues), "response should contain 1 missing value")
	assert.Equal(t, "A", results.GetIncompleteInvocationContext().MissingValues[0].StateName, "response should register state A")
}

func TestModifiedStateRegistration(t *testing.T) {

	providedStates := []*messages.ToFunction_PersistedValue{
		{
			StateName:  "B",
			StateValue: nil,
		},
	}

	statesSpecs := map[string]*messages.FromFunction_PersistedValueSpec{
		"A": {
			StateName: "A",
		},
		"B": {
			StateName: "B",
		},
	}

	results := checkRegisteredStates(providedStates, statesSpecs)
	assert.NotNil(t, results, "missing states should not be nil")
	assert.Equal(t, 1, len(results.GetIncompleteInvocationContext().MissingValues), "response should contain 1 missing value")
	assert.Equal(t, "A", results.GetIncompleteInvocationContext().MissingValues[0].StateName, "response should register state A")
}

func TestUnModifiedStateRegistration(t *testing.T) {

	providedStates := []*messages.ToFunction_PersistedValue{
		{
			StateName: "A",
		},
	}

	statesSpecs := map[string]*messages.FromFunction_PersistedValueSpec{
		"A": {
			StateName: "A",
		},
	}

	results := checkRegisteredStates(providedStates, statesSpecs)
	assert.Nil(t, results, "no response should be provided when all states are provided")
}
