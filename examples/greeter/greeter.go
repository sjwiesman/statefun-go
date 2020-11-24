package main

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/types/known/anypb"
	"net/http"

	"github.com/sjwiesman/statefun-go/pkg/flink/statefun"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/io"
)

var egressId = io.EgressIdentifier{
	EgressNamespace: "example",
	EgressType:      "greets",
}

type Greeter struct{}

func (greeter Greeter) StateSpecs() []statefun.StateSpec {
	return []statefun.StateSpec{
		{
			StateName: "seen_count",
		},
	}
}

func (greeter Greeter) Invoke(ctx context.Context, runtime statefun.StatefulFunctionRuntime, _ *anypb.Any) error {
	var seen SeenCount
	if _, err := runtime.Get("seen_count", &seen); err != nil {
		return err
	}

	seen.Seen += 1

	if err := runtime.Set("seen_count", &seen); err != nil {
		return err
	}

	self := statefun.Self(ctx)
	response := computeGreeting(self.Id, seen.Seen)

	record := io.KafkaRecord{
		Topic: "greetings",
		Key:   statefun.Self(ctx).Id,
		Value: response,
	}

	message, err := record.ToMessage()
	if err != nil {
		return nil
	}

	return runtime.SendEgress(egressId, message)
}

func computeGreeting(id string, seen int64) *GreetResponse {
	templates := []string{
		"",
		"Welcome %s",
		"Nice to see you again %s",
		"Third time is a charm %s"}

	greeting := &GreetResponse{}
	if int(seen) < len(templates) {
		greeting.Greeting = fmt.Sprintf(templates[seen], id)
	} else {
		greeting.Greeting = fmt.Sprintf("Nice to see you at the %d-nth time %s!", seen, id)
	}

	return greeting
}

func main() {
	registry := statefun.NewFunctionRegistry()
	registry.RegisterFunction(statefun.FunctionType{
		Namespace: "example",
		Type:      "greeter",
	}, Greeter{})

	http.Handle("/statefun", registry)
	_ = http.ListenAndServe(":8000", nil)
}
