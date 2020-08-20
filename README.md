# Statefun Go SDK

An SDK for writing stateful functions in Go. See the [Apache Flink Stateful
Functions](https://flink.apache.org/stateful-functions.html) website for more
information about the project.

The following shows how to write a simple stateful function and serve it for use in a Statefun deployment.

```go

var egressId = EgressIdentifier {
    EgressNamespace: "example",
    EgressType: "greets",
} 

struct Greeter{}

func (greeter Greeter) Invoke(io StatefulFunctionIO, msg *any.Any) error {
    var seen SeenCount
    if err := io.Get("seen-count", &seen); err != nil {
        return err
    }
    
    seen.seen += 1
    
    if err := io.Set("seen-count", &seen); err != nil {
        return err
    }
    
    response := computeGreeting()
    
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

	http.Handle("/statefun", functions)
	_ = http.ListenAndServe(":8000", nil)
}
```



This is a simple example that runs a simple stateful function that accepts requests from a Kafka ingress, and then responds by sending greeting responses to a Kafka egress.
It demonstrates the primitive building blocks of a Stateful Functions applications, such as ingresses, handling state in functions, and sending messages to egresses.

Refer to the Stateful Functions documentation to learn how to use this in a deployment.
Especially the modules documentation is pertinent.

**WARNING** This library is still in alpha and developers reserve the right to 
make breaking changes to the api. 
