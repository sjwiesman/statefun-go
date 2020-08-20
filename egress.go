package statefun_go

import "fmt"

// A reference to an egress, consisting of a namespace and a name.
type EgressIdentifier struct {
	EgressNamespace string
	EgressType      string
}

func (egress *EgressIdentifier) String() string {
	return fmt.Sprintf("%s/%s", egress.EgressNamespace, egress.EgressType)
}
