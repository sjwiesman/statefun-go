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
