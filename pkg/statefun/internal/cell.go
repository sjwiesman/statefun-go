package internal

import (
	"bytes"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type Cell struct {
	typedValue *protocol.TypedValue
	buffer     bytes.Buffer
	mutated    bool
}

func NewCell(state *protocol.ToFunction_PersistedValue) *Cell {
	c := &Cell{
		typedValue: state.StateValue,
	}

	_, _ = c.buffer.Read(c.typedValue.Value)
	c.typedValue.Value = nil
	return c
}

func (c *Cell) Read(p []byte) (n int, err error) {
	return c.buffer.Read(p)
}

func (c *Cell) Write(p []byte) (n int, err error) {
	c.buffer.Reset()
	c.mutated = true
	c.typedValue.HasValue = true
	return c.buffer.Write(p)
}

func (c *Cell) Reset() {
	c.mutated = true
	c.typedValue.HasValue = false
	c.buffer.Reset()
}

func (c Cell) HasValue() bool {
	return c.typedValue.HasValue
}

func (c Cell) GetStateMutation(name string) *protocol.FromFunction_PersistedValueMutation {
	if !c.mutated {
		return nil
	}

	mutationType := protocol.FromFunction_PersistedValueMutation_DELETE
	if c.typedValue.HasValue {
		mutationType = protocol.FromFunction_PersistedValueMutation_MODIFY
		c.typedValue.Value = c.buffer.Bytes()
	}

	return &protocol.FromFunction_PersistedValueMutation{
		MutationType: mutationType,
		StateName:    name,
		StateValue:   c.typedValue,
	}
}
