package statefun

import (
	"fmt"
	"time"
)

type expirationType int

const (
	expireAfterCall expirationType = iota
	expireAfterWrite
	none
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

var noExpiration = &Expiration{
	none,
	time.Duration(0),
}

func (e *Expiration) String() string {
	if e == nil {
		e = noExpiration
	}

	return fmt.Sprintf("Expiration{mode=%v, duration=%v}", e.expirationType.String(), e.duration.String())
}

// Returns an Expiration configuration that would expire
// a duration after the last invocation of the Function.
func ExpireAfterCall(duration time.Duration) *Expiration {
	return &Expiration{
		expireAfterCall,
		duration,
	}
}

// Returns an Expiration configuration that would expire
// a duration after the last write.
func ExpireAfterWrite(duration time.Duration) *Expiration {
	return &Expiration{
		expireAfterWrite,
		duration,
	}
}

type ValueSpec struct {
	Name       string
	Expiration *Expiration
	ValueType  Type
}
