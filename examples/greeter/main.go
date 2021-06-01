package main

import (
	"fmt"
	"log"
	"net/http"
	"statefun-sdk-go/pkg/statefun"
)

type GreetRequest struct {
	Name   string `json:"name"`
	Visits int32  `json:"visits"`
}

var GreetRequestType = statefun.MakeJsonType(statefun.TypeNameFrom("example/GreetRequest"))

var PersonFunc = statefun.TypeNameFrom("example/person")

var GreeterFunc = statefun.TypeNameFrom("example/greeter")

var KafkaEgress = statefun.TypeNameFrom("example/greets")

type Person struct {
	Visits statefun.ValueSpec
}

func (p Person) Invoke(ctx statefun.Context, msg statefun.Message) error {

	var visits int32
	_ = ctx.Storage().Get(p.Visits, &visits)
	visits += 1
	ctx.Storage().Set(p.Visits, visits)

	request := GreetRequest{}
	_ = msg.As(GreetRequestType, &request)
	request.Visits = visits

	ctx.Send(statefun.MessageBuilder{
		Target: statefun.Address{
			FunctionType: GreeterFunc,
			Id:           request.Name,
		},
		Value:     request,
		ValueType: GreetRequestType,
	})

	return nil
}

func greeter(ctx statefun.Context, msg statefun.Message) error {

	var request GreetRequest
	_ = msg.As(GreetRequestType, &request)

	greeting := computeGreeting(request.Name, request.Visits)

	ctx.SendEgress(statefun.KafkaEgressBuilder{
		Target: KafkaEgress,
		Topic:  "greetings",
		Key:    request.Name,
		Value:  greeting,
	})

	return nil
}

func computeGreeting(name string, seen int32) string {
	templates := []string{"", "Welcome %s", "Nice to see you again %s", "Third time is the charm %s"}
	if int(seen) < len(templates) {
		return fmt.Sprintf(templates[seen], name)
	}

	return fmt.Sprintf("Nice to see you for the %dth time %s", seen, name)
}

func main() {

	builder := statefun.StatefulFunctionsBuilder()

	person := Person{
		Visits: statefun.ValueSpec{
			Name:      "visits",
			ValueType: statefun.Int32Type,
		}}

	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: PersonFunc,
		States:       []statefun.ValueSpec{person.Visits},
		Function:     person,
	})

	_ = builder.WithSpec(statefun.StatefulFunctionSpec{
		FunctionType: GreeterFunc,
		Function:     statefun.StatefulFunctionPointer(greeter),
	})

	http.Handle("/statefun", builder.AsHandler())
	log.Fatal(http.ListenAndServe(":8000", nil))
}
