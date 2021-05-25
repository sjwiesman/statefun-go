package statefun

import (
	"bytes"
	"context"
	"fmt"
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

func greeter(ctx context.Context, storage AddressScopedStorage, msg Message) error {
	if msg.IsString() {
		if _, err := msg.AsString(); err != nil {
			return fmt.Errorf("failed to deserialize message: %w", err)
		}
	}

	var seen int32
	if _, err := storage.Get(Seen, &seen); err != nil {
		return fmt.Errorf("failed to read state: %w", err)
	}

	seen += 1

	if err := storage.Set(Seen, seen); err != nil {
		return fmt.Errorf("failed to write state: %w", err)
	}

	mailbox := Mailbox(ctx)

	mailbox <- MessageBuilder{
		Target: Address{
			TypeName: TypeNameFrom("org.foo/greeter-java"),
			Id:       "0",
		},
		Value: seen,
	}

	mailbox <- MessageBuilder{
		Target: Address{
			TypeName: TypeNameFrom("night/owl"),
			Id:       "1",
		},
		Value: "hoo hoo",
		Delay: time.Duration(1) * time.Hour,
	}

	mailbox <- KafkaEgressBuilder{
		Target: TypeNameFrom("e/kafka"),
		Topic:  "out",
		Key:    "abc",
		Value:  int32(133742),
	}

	mailbox <- KinesisEgressBuilder{
		Target:       TypeNameFrom("e/kinesis"),
		Stream:       "out",
		Value:        "hello there",
		PartitionKey: "abc",
	}

	return nil
}

func TestMissingStateValues(t *testing.T) {
	builder := StatefulFunctionsBuilder()
	builder.WithSpec(StatefulFunctionSpec{
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
	builder.WithSpec(StatefulFunctionSpec{
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

func toTypedValue(valueType Type, value interface{}) *protocol.TypedValue {
	data, err := valueType.Serialize(value)
	if err != nil {
		panic(err)
	}

	return &protocol.TypedValue{
		Typename: valueType.GetTypeName().String(),
		HasValue: true,
		Value:    data,
	}
}
