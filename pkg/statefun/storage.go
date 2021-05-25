package statefun

import (
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

type AddressScopedStorage interface {
	Get(spec ValueSpec, receiver interface{}) (bool, error)

	Set(spec ValueSpec, value interface{}) error

	Clear(spec ValueSpec) error
}

type storage struct {
	mutex   sync.RWMutex
	states  map[string]*protocol.TypedValue
	mutated map[string]bool
}

func (s *storage) Get(spec ValueSpec, receiver interface{}) (bool, error) {
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

func (s *storage) Set(spec ValueSpec, value interface{}) error {
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

func (s *storage) Clear(spec ValueSpec) error {
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
