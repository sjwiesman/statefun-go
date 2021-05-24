package statefun

import (
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

type AddressScopedStorage struct {
	mutex   sync.RWMutex
	states  map[string]*protocol.TypedValue
	mutated map[string]bool
}

func (s *AddressScopedStorage) Get(spec ValueSpec, receiver interface{}) (bool, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	typedValue, ok := s.states[spec.Name]
	if !ok {
		return false, fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	if !typedValue.HasValue {
		return false, nil
	}

	return true, spec.ValueType.Deserialize(receiver, typedValue.Value)
}

func (s *AddressScopedStorage) Set(spec ValueSpec, value interface{}) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	typedValue, ok := s.states[spec.Name]
	if !ok {
		return fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	data, err := spec.ValueType.Serialize(value)
	if err != nil {
		return err
	}

	typedValue.HasValue = true
	typedValue.Value = data
	s.mutated[spec.Name] = true

	return nil
}

func (s *AddressScopedStorage) Clear(spec ValueSpec) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	typedValue, ok := s.states[spec.Name]
	if !ok {
		return fmt.Errorf("unregistered ValueSpec %s", spec.Name)
	}

	typedValue.HasValue = false
	typedValue.Value = nil
	s.mutated[spec.Name] = true

	return nil
}
