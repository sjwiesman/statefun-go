package statefun

import (
	"bytes"
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

type AddressScopedStorage interface {
	Get(spec ValueSpec, receiver interface{}) bool

	Set(spec ValueSpec, value interface{})

	Clear(spec ValueSpec)
}

type cell struct {
	typedValue *protocol.TypedValue
	buffer     bytes.Buffer
	mutated    bool
}

func newCell(state *protocol.ToFunction_PersistedValue) cell {
	c := cell{
		typedValue: state.StateValue,
	}

	_, _ = c.buffer.Read(c.typedValue.Value)
	c.typedValue.Value = nil
	return c
}

func (c cell) getStateMutation(name string) *protocol.FromFunction_PersistedValueMutation {
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

type storage struct {
	mutex sync.RWMutex
	cells map[string]cell
}

type StorageFactory interface {
	GetStorage() *storage

	GetMissingSpecs() []*protocol.FromFunction_PersistedValueSpec
}

func NewStorageFactory(
	batch *protocol.ToFunction_InvocationBatchRequest,
	specs map[string]*protocol.FromFunction_PersistedValueSpec,
) StorageFactory {
	storage := &storage{
		mutex: sync.RWMutex{},
		cells: make(map[string]cell, len(specs)),
	}

	states := make(map[string]*protocol.FromFunction_PersistedValueSpec, len(specs))
	for k, v := range specs {
		states[k] = v
	}

	for _, state := range batch.State {
		if _, exists := states[state.StateName]; !exists {
			continue
		}

		delete(states, state.StateName)

		storage.cells[state.StateName] = newCell(state)
	}

	if len(states) > 0 {
		var missing = make([]*protocol.FromFunction_PersistedValueSpec, 0, len(states))
		for _, spec := range states {
			missing = append(missing, spec)
		}

		return MissingSpecs(missing)
	} else {
		return storage
	}
}

func (s *storage) GetStorage() *storage {
	return s
}

func (s *storage) GetMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return nil
}

func (s *storage) Get(spec ValueSpec, receiver interface{}) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	if !cell.typedValue.HasValue {
		return false
	}

	if err := spec.ValueType.Deserialize(&cell.buffer, receiver); err != nil {
		panic(fmt.Errorf("failed to deserialize %s: %w", spec.Name, err))
	}

	return true
}

func (s *storage) Set(spec ValueSpec, value interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	cell.buffer.Reset()
	err := spec.ValueType.Serialize(&cell.buffer, value)
	if err != nil {
		panic(fmt.Errorf("failed to serialize %s: %w", spec.Name, err))
	}

	cell.typedValue.HasValue = true
	cell.mutated = true
	s.cells[spec.Name] = cell
}

func (s *storage) Clear(spec ValueSpec) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	cell.buffer.Reset()
	cell.typedValue.HasValue = false
	cell.mutated = true
}

func (s *storage) getStateMutations() []*protocol.FromFunction_PersistedValueMutation {
	mutations := make([]*protocol.FromFunction_PersistedValueMutation, 0)
	for name, cell := range s.cells {
		mutation := cell.getStateMutation(name)
		if mutation != nil {
			mutations = append(mutations, mutation)
		}
	}

	return mutations
}

type MissingSpecs []*protocol.FromFunction_PersistedValueSpec

func (m MissingSpecs) GetStorage() *storage {
	return nil
}

func (m MissingSpecs) GetMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return m
}
