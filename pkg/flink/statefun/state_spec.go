package statefun

import (
	"fmt"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"log"
	"time"
)

type ExpireMode int

const (
	NONE ExpireMode = iota
	AFTER_WRITE
	AFTER_INVOKE
)

func (e ExpireMode) String() string {
	switch e {
	case NONE:
		return "NONE"
	case AFTER_WRITE:
		return "AFTER_WRITE"
	case AFTER_INVOKE:
		return "AFTER_INVOKE"
	default:
		return "UNKNOWN"
	}
}

func (e ExpireMode) toInternal() messages.FromFunction_ExpirationSpec_ExpireMode {
	switch e {
	case NONE:
		return messages.FromFunction_ExpirationSpec_NONE
	case AFTER_WRITE:
		return messages.FromFunction_ExpirationSpec_AFTER_WRITE
	case AFTER_INVOKE:
		return messages.FromFunction_ExpirationSpec_AFTER_INVOKE
	default:
		log.Panicf("unknown expiration mode %v", e)
		// this return is just to make the compiler happy
		// it will never be reached
		return 0
	}
}

type ExpirationSpec struct {
	ExpireMode
	Ttl time.Duration
}

func (e ExpirationSpec) String() string {
	return fmt.Sprintf("ExpirationSpec{ExpireMode=%v, Ttl=%v}", e.ExpireMode, e.Ttl)
}

type StateSpec struct {
	StateName string
	ExpirationSpec
}

func (s StateSpec) String() string {
	return fmt.Sprintf("StateSpec{StateName=%v, ExpirationSpec=%v}", s.StateName, s.ExpirationSpec)
}

func (s StateSpec) toInternal() *messages.FromFunction_PersistedValueSpec {
	return &messages.FromFunction_PersistedValueSpec{
		StateName: s.StateName,
		ExpirationSpec: &messages.FromFunction_ExpirationSpec{
			Mode:              s.ExpirationSpec.ExpireMode.toInternal(),
			ExpireAfterMillis: s.ExpirationSpec.Ttl.Milliseconds(),
		},
	}
}
