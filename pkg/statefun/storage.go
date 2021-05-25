package statefun

import (
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

type AddressScopedStorage interface {
	Get(spec ValueSpec, receiver interface{}) bool

	Set(spec ValueSpec, value interface{})

	Clear(spec ValueSpec)
}

type storage struct {
	mutex   sync.RWMutex
	states  map[string]*protocol.TypedValue
	mutated map[string]bool
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
		mutex:   sync.RWMutex{},
		states:  make(map[string]*protocol.TypedValue, len(specs)),
		mutated: make(map[string]bool, len(specs)),
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
		storage.states[state.StateName] = state.StateValue
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

	typedValue, ok := s.states[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	if !typedValue.HasValue {
		return false
	}

	if err := spec.ValueType.Deserialize(receiver, typedValue.Value); err != nil {
		panic(fmt.Errorf("failed to deserialize %s: %w", spec.Name, err))
	}

	return true
}

func (s *storage) Set(spec ValueSpec, value interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	typedValue, ok := s.states[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	data, err := spec.ValueType.Serialize(value)
	if err != nil {
		panic(fmt.Errorf("failed to serialize %s: %w", spec.Name, err))
	}

	typedValue.HasValue = true
	typedValue.Value = data
	s.mutated[spec.Name] = true
}

func (s *storage) Clear(spec ValueSpec) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	typedValue, ok := s.states[spec.Name]
	if !ok {
		panic(fmt.Errorf("unregistered ValueSpec %s", spec.Name))
	}

	typedValue.HasValue = false
	typedValue.Value = nil
	s.mutated[spec.Name] = true
}

type MissingSpecs []*protocol.FromFunction_PersistedValueSpec

func (m MissingSpecs) GetStorage() *storage {
	return nil
}

func (m MissingSpecs) GetMissingSpecs() []*protocol.FromFunction_PersistedValueSpec {
	return m
}
