package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	any "google.golang.org/protobuf/types/known/anypb"
	"net/http"
	"statefun-go/pkg/flink/statefun"
)

func randToken(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		panic("Fail :(")
	}
	return hex.EncodeToString(bytes)
}

type CounterFunction struct{}

func (c CounterFunction) Invoke(ctx statefun.StatefulFunctionIO, message *any.Any) error {
	var count InvokeCount
	if err := ctx.Get("invoke_count", &count); err != nil {
		return fmt.Errorf("unable to deserialize invoke_count %w", err)
	}

	count.Count += 1

	if err := ctx.Set("invoke_count", &count); err != nil {
		return fmt.Errorf("unable to serialize invoke_count %w", err)
	}

	response := &InvokeResult{
		InvokeCount: count.Count,
		Id:          ctx.Self().Id,
	}

	target := &statefun.Address{
		Namespace: "org.apache.flink.statefun.e2e.remote",
		Type:      "forward-function",
		Id:        randToken(0xFFFF),
	}

	return ctx.Send(target, response)
}

func ForwardFunction(ctx statefun.StatefulFunctionIO, message *any.Any) error {
	egress := statefun.EgressIdentifier{
		EgressNamespace: "org.apache.flink.statefun.e2e.remote",
		EgressType:      "invoke-results",
	}

	kafkaRecord, err := statefun.KafkaEgressRecord("invoke-results", ctx.Self().Id, message)
	if err != nil {
		return err
	}

	return ctx.SendEgress(egress, kafkaRecord)
}

func main() {
	functions := statefun.NewFunctionRegistry()
	functions.RegisterFunction(statefun.FunctionType{
		Namespace: "org.apache.flink.statefun.e2e.remote",
		Type:      "counter",
	}, CounterFunction{})

	functions.RegisterFunctionPointer(statefun.FunctionType{
		Namespace: "org.apache.flink.statefun.e2e.remote",
		Type:      "forward-function",
	}, ForwardFunction)

	http.Handle("/service", functions)
	_ = http.ListenAndServe(":8000", nil)
}
