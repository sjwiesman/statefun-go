package statefun

import "fmt"

type Egress struct {
	EgressNamespace string
	EgressType      string
}

func (egress *Egress) String() string {
	return fmt.Sprintf("%s/%s", egress.EgressNamespace, egress.EgressType)
}
