package statefun

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"testing"
	"time"
)

var Seen = ValueSpec{
	Name:      "seen",
	ValueType: Int32Type,
}

func greeter(ctx Context, msg Message) error {
	if msg.IsString() {
		_ = msg.AsString()
	}

	var seen int32
	ctx.Storage().Get(Seen, &seen)
	seen += 1
	ctx.Storage().Set(Seen, seen)

	ctx.Send(MessageBuilder{
		Target: Address{
			FunctionType: TypeNameFrom("org.foo/greeter-java"),
			Id:           "0",
		},
		Value: seen,
	})

	ctx.SendAfter(time.Duration(1)*time.Hour, MessageBuilder{
		Target: Address{
			FunctionType: TypeNameFrom("night/owl"),
			Id:           "1",
		},
		Value: "hoo hoo",
	})

	ctx.SendEgress(KafkaEgressBuilder{
		Target: TypeNameFrom("e/kafka"),
		Topic:  "out",
		Key:    "abc",
		Value:  int32(133742),
	})

	ctx.SendEgress(KinesisEgressBuilder{
		Target:       TypeNameFrom("e/kinesis"),
		Stream:       "out",
		Value:        "hello there",
		PartitionKey: "abc",
	})

	return nil
}

func TestMissingStateValues(t *testing.T) {
	builder := StatefulFunctionsBuilder()
	err := builder.WithSpec(StatefulFunctionSpec{
		FunctionType: TypeNameFrom("org.foo/greeter"),
		States:       []ValueSpec{Seen},
		Function:     StatefulFunctionPointer(greeter),
	})

	assert.NoError(t, err, "registering a function should succeed")

	server := httptest.NewServer(builder.AsHandler())
	defer server.Close()

	toFunction := protocol.ToFunction{
		Request: &protocol.ToFunction_Invocation_{
			Invocation: &protocol.ToFunction_InvocationBatchRequest{
				Target: &protocol.Address{
					Namespace: "org.foo",
					Type:      "greeter",
					Id:        "0",
				},
				State: nil,
				Invocations: []*protocol.ToFunction_Invocation{
					{
						Caller:   nil,
						Argument: toTypedValue(StringType, "Hello"),
					},
				},
			},
		},
	}

	request, _ := proto.Marshal(&toFunction)
	response, err := http.Post(server.URL, "application/octet-stream", bytes.NewReader(request))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode, "received non-200 response")

	var from protocol.FromFunction
	responsebody, _ := ioutil.ReadAll(response.Body)
	_ = proto.Unmarshal(responsebody, &from)

	ctx := from.GetIncompleteInvocationContext()
	assert.NotNil(t, ctx, "missing state context should not be nil")
	assert.Equal(t, "seen", ctx.MissingValues[0].StateName)
}

func TestHandler(t *testing.T) {
	builder := StatefulFunctionsBuilder()
	err := builder.WithSpec(StatefulFunctionSpec{
		FunctionType: TypeNameFrom("org.foo/greeter"),
		States:       []ValueSpec{Seen},
		Function:     StatefulFunctionPointer(greeter),
	})

	assert.NoError(t, err, "registering a function should succeed")
	server := httptest.NewServer(builder.AsHandler())
	defer server.Close()

	toFunction := protocol.ToFunction{
		Request: &protocol.ToFunction_Invocation_{
			Invocation: &protocol.ToFunction_InvocationBatchRequest{
				Target: &protocol.Address{
					Namespace: "org.foo",
					Type:      "greeter",
					Id:        "0",
				},
				State: []*protocol.ToFunction_PersistedValue{
					{
						StateName: "seen",
						StateValue: &protocol.TypedValue{
							Typename: "io.statefun.types/int",
							HasValue: false,
							Value:    nil,
						},
					},
				},
				Invocations: []*protocol.ToFunction_Invocation{
					{
						Caller:   nil,
						Argument: toTypedValue(StringType, "Hello"),
					},
				},
			},
		},
	}

	request, _ := proto.Marshal(&toFunction)
	response, err := http.Post(server.URL, "application/octet-stream", bytes.NewReader(request))

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode, "received non-200 response")

	var from protocol.FromFunction
	responsebody, _ := ioutil.ReadAll(response.Body)
	_ = proto.Unmarshal(responsebody, &from)

	result := from.GetInvocationResult()
	assert.NotNil(t, result, "invocation result should not be nil")

	assert.Equal(t, "seen", result.StateMutations[0].StateName)
	assert.Equal(t, protocol.FromFunction_PersistedValueMutation_MODIFY, result.StateMutations[0].MutationType)

	assert.Equal(t, &protocol.Address{
		Namespace: "org.foo",
		Type:      "greeter-java",
		Id:        "0",
	}, result.OutgoingMessages[0].Target)
	assert.Equal(t, "io.statefun.types/int", result.OutgoingMessages[0].Argument.Typename)

	assert.Equal(t, int64(1000*60*60), result.DelayedInvocations[0].DelayInMs)
	assert.Equal(t, "io.statefun.types/string", result.DelayedInvocations[0].Argument.Typename)

	assert.Equal(t, "e", result.OutgoingEgresses[0].EgressNamespace)
	assert.Equal(t, "kafka", result.OutgoingEgresses[0].EgressType)
	assert.Equal(t, "type.googleapis.com/io.statefun.sdk.egress.KafkaProducerRecord", result.OutgoingEgresses[0].Argument.Typename)
}

func BenchmarkHandler(t *testing.B) {
	builder := StatefulFunctionsBuilder()
	_ = builder.WithSpec(StatefulFunctionSpec{
		FunctionType: TypeNameFrom("org.foo/greeter"),
		States:       []ValueSpec{Seen},
		Function:     StatefulFunctionPointer(greeter),
	})

	server := httptest.NewServer(builder.AsHandler())
	defer server.Close()

	toFunction := protocol.ToFunction{
		Request: &protocol.ToFunction_Invocation_{
			Invocation: &protocol.ToFunction_InvocationBatchRequest{
				Target: &protocol.Address{
					Namespace: "org.foo",
					Type:      "greeter",
					Id:        "0",
				},
				State: []*protocol.ToFunction_PersistedValue{
					{
						StateName: "seen",
						StateValue: &protocol.TypedValue{
							Typename: "io.statefun.types/int",
							HasValue: false,
							Value:    nil,
						},
					},
				},
				Invocations: []*protocol.ToFunction_Invocation{
					{
						Caller:   nil,
						Argument: toTypedValue(StringType, "Hello"),
					},
				},
			},
		},
	}

	request, _ := proto.Marshal(&toFunction)

	t.ReportAllocs()

	for i := 0; i < t.N; i++ {
		response, _ := http.Post(server.URL, "application/octet-stream", bytes.NewReader(request))
		_ = response.Body.Close()
	}
}

func toTypedValue(valueType SimpleType, value interface{}) *protocol.TypedValue {
	buffer := bytes.Buffer{}
	if err := valueType.Serialize(&buffer, value); err != nil {
		panic(err)
	}

	return &protocol.TypedValue{
		Typename: valueType.GetTypeName().String(),
		HasValue: true,
		Value:    buffer.Bytes(),
	}
}
