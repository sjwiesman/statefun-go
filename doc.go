// Stateful Functions Go SDK
//
// Stateful Functions is an API that simplifies the building of **distributed stateful applications** with
// a **runtime built for serverless architectures**. It brings together the benefits of stateful stream
// processing, the processing of large datasets with low latency and bounded resource constraints,
// along with a runtime for modeling stateful entities that supports location transparency, concurrency,
// scaling, and resiliency.
//
// It is designed to work with modern architectures, like cloud-native deployments and popular event-driven FaaS platforms
// like AWS Lambda and KNative, and to provide out-of-the-box consistent state and messaging while preserving the serverless
// experience and elasticity of these platforms.
//
// Background
//
// The JVM-based Stateful Functions implementation has a RequestReply extension
// (a protocol and an implementation) that allows calling into any HTTP endpoint
// that implements that protocol. Although it is possible to implement this protocol
// independently, this is a minimal library for the Go programing language that:
//
// - Allows users to define and declare their functions in a convenient way.
//
// - Dispatches an invocation request sent from the JVM to the appropriate function previously declared.
//
// A Mini-Tutorial
//
// This is a simple example that runs a simple stateful function that accepts requests from a Kafka ingress,
// and then responds by sending greeting responses to a Kafka egress.
// It demonstrates the primitive building blocks of a Stateful Functions applications, such as ingresses,
// handling state in functions, and sending messages to egresses.
//
//   import (
//   	"fmt"
//   	"github.com/golang/protobuf/ptypes/any"
//   	"net/http"
//   )
//
//   var egressId = EgressIdentifier {
//   	EgressNamespace: "example",
//   	EgressType: "greets",
//   }
//
// Define a simple stateful function that tracks how many times
// each user of the system has been seen and generates a custom
// greeting based on that information. Each user is tracked
// independently and the runtime guarantees exactly-once
// state semantics.
//
//   type Greeter struct {}
//
//   func (greeter Greeter) Invoke(io StatefulFunctionIO, msg *any.Any) error {
//   	var seen SeenCount
//   	if err := io.Get("seen-count", &seen); err != nil {
//   		return err
//   	}
//
//   	seen.seen += 1
//
//   	if err := io.Set("seen-count", &seen); err != nil {
//   		return err
//   	}
//
//   	response := computeGreeting(io.Self().Id, seen.seen)
//
//   	egressMessage := KafkaEgressRecord("greeting", io.Self().Id, response)
//   	return io.SendEgress(egressId, egressMessage)
//
//   }
//
//   func computeGreeting(id string, seen int) Greeting {
//   	templates := []string{
//  		"",
// 			"Welcome %s",
//			"Nice to see you again %s",
//			"Third time is a charm %s"}
//
//   	var greeting Greeting
//   	if seen < len(templates) {
//   		Greeting.msg = fmt.Sprintf(templates[seen], id)
//   	} else {
//   		Greeting.msg = fmt.Sprintf("Nice to see you at the %d-nth time %s!", seen, id)
//   	}
//
//   	return greeting
//   }
//
//   func main() {
//   	registry := NewFunctionRegistry()
//   	registry.RegisterFunction(FunctionType{
//   		Namespace: "example",
//   		Type:      "greeter",
//   	}, Greeter{})
//
//   	http.Handle("/statefun", registry)
//   	_ = http.ListenAndServe(":8000", nil)
//   }
package statefun_go
