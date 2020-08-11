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

func (c CounterFunction) Invoke(_ *any.Any, ctx *statefun.context) error {
	var count InvokeCount
	if err := ctx.GetAndUnpack("invoke_count", &count); err != nil {
		return fmt.Errorf("unable to deserialize invoke_count %w", err)
	}

	count.Count += 1

	if err := ctx.SetAndPack("invoke_count", &count); err != nil {
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

	return ctx.SendAndPack(target, response)
}

func ForwardFunction(message *any.Any, ctx *statefun.context) error {
	egress := statefun.Egress{
		EgressNamespace: "org.apache.flink.statefun.e2e.remote",
		EgressType:      "invoke-results",
	}

	kafkaRecord, err := statefun.KafkaEgressRecord("invoke-results", ctx.Self().Id, message)
	if err != nil {
		return err
	}

	return ctx.SendEgressAndPack(egress, kafkaRecord)
}

func main() {
	functions := statefun.NewStatefulFunctions()
	functions.StatefulFunction(statefun.FunctionType{
		Namespace: "org.apache.flink.statefun.e2e.remote",
		Type:      "counter",
	}, CounterFunction{})

	functions.StatefulFunctionPointer(statefun.FunctionType{
		Namespace: "org.apache.flink.statefun.e2e.remote",
		Type:      "forward-function",
	}, ForwardFunction)

	http.Handle("/service", functions)
	_ = http.ListenAndServe(":8000", nil)
}
