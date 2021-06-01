package statefun

import (
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

// An AddressScopedStorage is used for reading and writing persistent
// values that are managed by the Stateful Functions runtime for
// fault-tolerance and consistency.
//
// All access to the storage is scoped to the current function instance,
// identified by the instance's Address. This means that within an
// invocation, function instances may only access its own persisted
// values through this storage.
type AddressScopedStorage interface {

	// Gets the values of the provided ValueSpec, scoped to the
	// current invoked Address and stores the result in the value
	// pointed to by receiver. The method will return false
	// if there is no value for the spec in storage
	// so callers can differentiate between missing and
	// the types zero value.
	Get(spec ValueSpec, receiver interface{}) (exists bool)

	// Sets the value for the provided ValueSpec, scoped
	// to the current invoked Address.
	Set(spec ValueSpec, value interface{})

	// Removes the prior value set for the the provided
	// ValueSpec, scoped to the current invoked Address.
	//
	// After removing the value, calling Get for the same
	// spec under the same Address will return false.
	Remove(spec ValueSpec)
}

type storage struct {
	mutex sync.RWMutex
	cells map[string]*internal.Cell
}

type storageFactory interface {
	getStorage() *storage

	getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec
}

func newStorageFactory(
	batch *protocol.ToFunction_InvocationBatchRequest,
	specs map[string]*protocol.FromFunction_PersistedValueSpec,
) storageFactory {
	storage := &storage{
		cells: make(map[string]*internal.Cell, len(specs)),
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

		storage.cells[state.StateName] = internal.NewCell(state)
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

func (s *storage) getStorage() *storage {
	return s
}

func (s *storage) getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return nil
}

func (s *storage) Get(spec ValueSpec, receiver interface{}) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	if !cell.HasValue() {
		return false
	}

	if err := spec.ValueType.Deserialize(cell, receiver); err != nil {
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

	err := spec.ValueType.Serialize(cell, value)
	if err != nil {
		panic(fmt.Errorf("failed to serialize %s: %w", spec.Name, err))
	}
}

func (s *storage) Remove(spec ValueSpec) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cell, ok := s.cells[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	cell.Reset()
}

func (s *storage) getStateMutations() []*protocol.FromFunction_PersistedValueMutation {
	mutations := make([]*protocol.FromFunction_PersistedValueMutation, 0)
	for name, cell := range s.cells {
		if mutation := cell.GetStateMutation(name); mutation != nil {
			mutations = append(mutations, mutation)
		}
	}

	return mutations
}

type MissingSpecs []*protocol.FromFunction_PersistedValueSpec

func (m MissingSpecs) getStorage() *storage {
	return nil
}

func (m MissingSpecs) getMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return m
}
