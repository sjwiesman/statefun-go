# The Greeter Example

This is a simple example that runs a simple stateful function that accepts requests from a Kafka ingress,
and then responds by sending greeting responses to a Kafka egress. It demonstrates the primitive building blocks
of a Stateful Functions applications, such as ingresses, handling state in functions,
and sending messages to egresses.

## Running the example

To run the example:

```
docker-compose build
docker-compose up -d
```

Then, to see the example in actions, see what comes out of the topic `greetings`:

```
docker-compose exec kafka-broker kafka-console-consumer.sh --bootstrap-server kafka-broker:9092 --from-beginning --topic greetings
```

