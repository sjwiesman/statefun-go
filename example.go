package statefun_go

import (
	"fmt"
	"github.com/golang/protobuf/ptypes/any"
	"net/http"
)

var egressId = EgressIdentifier {
	EgressNamespace: "example",
	EgressType: "greets",
}

type Greeter struct {}

func (greeter Greeter) Invoke(io StatefulFunctionIO, msg *any.Any) error {
	var seen SeenCount
	if err := io.Get("seen-count", &seen); err != nil {
		return err
	}

	seen.seen += 1

	if err := io.Set("seen-count", &seen); err != nil {
		return err
	}

	response := computeGreeting(io.Self().Id, seen.seen)

	egressMessage := KafkaEgressRecord("greeting", io.Self().Id, seen)
	err := io.SendEgress(egressId, egressMessage)

	return err
}

func computeGreeting(id string, seen int) Greeting {
	templates := []string{"", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"}

	var greeting Greeting
	if seen < len(templates) {
		Greeting.msg = fmt.Sprintf(templates[seen], id)
	} else {
		Greeting.msg = fmt.Sprintf("Nice to see you at the %d-nth time %s!", seen, id)
	}

	return greeting
}

func main() {
	registry := NewFunctionRegistry()
	registry.RegisterFunction(FunctionType{
		Namespace: "example",
		Type:      "greeter",
	}, Greeter{})

	http.Handle("/statefun", registry)
	_ = http.ListenAndServe(":8000", nil)
}
