package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	. "statefun-sdk-go/pkg/statefun"
)

type GreetRequest struct {
	Name   string `json:"name"`
	Visits int32  `json:"visits"`
}

var GreetRequestType = MakeJsonType(TypeNameFrom("example/GreetRequest"))

var PersonFunc = TypeNameFrom("example/person")

var GreeterFunc = TypeNameFrom("example/greeter")

var KafkaEgress = TypeNameFrom("example/greets")

type Person struct {
	Visits ValueSpec
}

func (p Person) Invoke(
	_ context.Context,
	storage *AddressScopedStorage,
	msg Message) (*Mailbox, error) {

	var visits int32
	_, _ = storage.Get(p.Visits, &visits)
	visits += 1
	_ = storage.Set(p.Visits, visits)

	request := GreetRequest{}
	_ = msg.As(GreetRequestType, &request)
	request.Visits = visits

	message, _ := MessageBuilder{
		Value:     request,
		ValueType: GreetRequestType,
	}.ToMessage(Address{
		TypeName: GreeterFunc,
		Id:       request.Name,
	})

	mailbox := &Mailbox{}
	mailbox.Send(message)
	return mailbox, nil
}

func greeter(
	_ context.Context,
	_ *AddressScopedStorage,
	msg Message) (*Mailbox, error) {

	var request GreetRequest
	_ = msg.As(GreetRequestType, &request)

	greeting := computeGreeting(request.Name, request.Visits)

	record, _ := KafkaEgressBuilder{
		Topic: "greetings",
		Key:   request.Name,
		Value: greeting,
	}.ToEgressMessage(KafkaEgress)

	mailbox := &Mailbox{}
	mailbox.SendEgress(record)

	return mailbox, nil
}

func computeGreeting(name string, seen int32) string {
	templates := []string{"", "Welcome %s", "Nice to see you again %s", "Third time is the charm %s"}
	if int(seen) < len(templates) {
		return fmt.Sprintf(templates[seen], name)
	}

	return fmt.Sprintf("Nice to see you for the %dth time %s", seen, name)
}

func main() {

	builder := StatefulFunctionsBuilder()

	person := Person{
		Visits: ValueSpec{
			Name:      "visits",
			ValueType: Int32Type,
		}}

	builder.WithSpec(StatefulFunctionSpec{
		FunctionType: PersonFunc,
		States:       []ValueSpec{person.Visits},
		Function:     person,
	})

	builder.WithSpec(StatefulFunctionSpec{
		FunctionType: GreeterFunc,
		Function:     StatefulFunctionPointer(greeter),
	})

	http.Handle("/statefun", builder.AsHandler())
	log.Fatal(http.ListenAndServe(":8000", nil))
}
