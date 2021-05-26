package statefun

import (
	"fmt"
	"log"
	"regexp"
	"time"
)

type expirationType int

const (
	none expirationType = iota
	expireAfterCall
	expireAfterWrite
)

func (e expirationType) String() string {
	switch e {
	case expireAfterCall:
		return "expire_after_call"
	case expireAfterWrite:
		return "expire_after_write"
	case none:
		return "none"
	default:
		panic("unknown Expiration type")
	}
}

// State Expiration Configuration
//
// Defines the way state can be auto expired by the runtime.
// State Expiration (also known as TTL) can be used to keep
// state from growing arbitrarily by assigning an Expiration
// date to a value.
//
// State can be expired after a duration has passed since either
// the last write to the state, or the last call to the Function.
type Expiration struct {
	expirationType
	duration time.Duration
}

func (e Expiration) String() string {
	return fmt.Sprintf("Expiration{mode=%v, duration=%v}", e.expirationType.String(), e.duration.String())
}

// Returns an Expiration configuration that would expire
// a duration after the last invocation of the Function.
func ExpireAfterCall(duration time.Duration) Expiration {
	return Expiration{
		expireAfterCall,
		duration,
	}
}

// Returns an Expiration configuration that would expire
// a duration after the last write.
func ExpireAfterWrite(duration time.Duration) Expiration {
	return Expiration{
		expireAfterWrite,
		duration,
	}
}

// A ValueSpec identifies a registered persistent value of a function, which will be
// managed by the Stateful Functions runtime for consistency and fault-tolerance. A
// ValueSpec is registered for a function by configuring it on the function's
// associated StatefulFunctionSpec.
type ValueSpec struct {
	// The given name of the persistent value. The name must be a valid
	// identifier conforming to the following rules:
	//
	// 1. First character must be an alphabet letter [a-z] / [A-Z], or an underscore '_'.
	// 2. Remaining characters can be an alphabet letter [a-z] / [A-Z], a digit [0-9], or
	//    an underscore '-'.
	// 3. Must not contain any spaces.
	Name string

	// The Type of the persistent value. Either
	// a built-in PrimitiveType or custom implementation.
	ValueType Type

	// An optional expiration configuration.
	Expiration Expiration
}

const invalidNameMessage = `
invalid state name %s. state names can only start with alphabet letters [a-z][A-Z] or an underscore '_' followed by zero or more characters that are alphanumeric or underscores
`

func validateValueSpec(s ValueSpec) error {
	matched, err := regexp.MatchString("^[a-zA-Z_][a-zA-Z_\\d]*$", s.Name)
	if err != nil {
		log.Panicf("invalid regex; this is a bug: %v", err)
	}

	if !matched {
		return fmt.Errorf(invalidNameMessage, s.Name)
	}

	return nil
}
