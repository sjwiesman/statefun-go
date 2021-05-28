package statefun

import (
	"bytes"
	"context"
	"fmt"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type TestRunner struct {
	Spec     StatefulFunctionSpec
	TargetId string
	Caller   Address
	states   map[string]*protocol.TypedValue
}

func (t *TestRunner) WithState(spec ValueSpec, value interface{}) error {
	if t.states == nil {
		t.states = make(map[string]*protocol.TypedValue)
	}

	buffer := bytes.Buffer{}
	if err := spec.ValueType.Serialize(&buffer, value); err != nil {
		return err
	}

	t.states[spec.Name] = &protocol.TypedValue{
		Typename: spec.ValueType.GetTypeName().String(),
		HasValue: true,
		Value:    buffer.Bytes(),
	}

	return nil
}

func (t *TestRunner) Run(msg Message) error {
	b := StatefulFunctionsBuilder()
	if err := b.WithSpec(t.Spec); err != nil {
		return err
	}

	h := b.AsHandler().(*handler)

	states := make([]*protocol.ToFunction_PersistedValue, 0)
	for name, value := range t.states {
		states = append(states, &protocol.ToFunction_PersistedValue{
			StateName:  name,
			StateValue: value,
		})
	}

	var caller *protocol.Address
	if t.Caller != (Address{}) {
		caller = &protocol.Address{
			Namespace: t.Caller.FunctionType.GetNamespace(),
			Type:      t.Caller.FunctionType.GetType(),
			Id:        t.Caller.Id,
		}
	}

	toFunction := &protocol.ToFunction{
		Request: &protocol.ToFunction_Invocation_{
			Invocation: &protocol.ToFunction_InvocationBatchRequest{
				Target: &protocol.Address{
					Namespace: t.Spec.FunctionType.GetNamespace(),
					Type:      t.Spec.FunctionType.GetType(),
					Id:        t.TargetId,
				},
				State: states,
				Invocations: []*protocol.ToFunction_Invocation{
					{
						Caller:   caller,
						Argument: msg.typedValue,
					},
				},
			},
		},
	}

	from, err := h.invoke(context.Background(), toFunction)

	if err != nil {
		return err
	}

	if incomplete := from.GetIncompleteInvocationContext(); incomplete != nil {
		for _, missing := range incomplete.MissingValues {
			return fmt.Errorf("state %s was not registered in the function state specification", missing.StateName)
		}
	}

	result := from.GetInvocationResult()
	for _, mutation := range result.StateMutations {
		t.states[mutation.StateName] = mutation.StateValue
	}

	return nil
}
