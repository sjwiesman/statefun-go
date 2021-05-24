package statefun

import (
	"context"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
	"sync"
)

type StatefulFunctions interface {
	WithSpec(spec StatefulFunctionSpec) StatefulFunctions

	AsHandler() RequestReplyHandler
}

type RequestReplyHandler interface {
	http.Handler

	// Handler for processing arbitrary payloads.
	// This method provides compliance with AWS Lambda
	// handler.
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
}

func StatefulFunctionsBuilder() StatefulFunctions {
	return &handler{
		module: map[TypeName]StatefulFunction{},
		stateSpecs: map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec{},
	}
}

type handler struct {
	module     map[TypeName]StatefulFunction
	stateSpecs map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec
}

func (h *handler) WithSpec(spec StatefulFunctionSpec) StatefulFunctions {
	h.module[spec.Id] = spec.Function
	h.stateSpecs[spec.Id] = make(map[string]*protocol.FromFunction_PersistedValueSpec, len(spec.States))

	for _, state := range spec.States {
		expiration := &protocol.FromFunction_ExpirationSpec{}
		if state.Expiration == nil {
			expiration.Mode = protocol.FromFunction_ExpirationSpec_NONE
			continue
		}
		switch state.Expiration.expirationType {
		case expireAfterWrite:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_WRITE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		case expireAfterCall:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_INVOKE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		}

		h.stateSpecs[spec.Id][state.Name] = &protocol.FromFunction_PersistedValueSpec{
			StateName:      state.Name,
			ExpirationSpec: expiration,
			TypeTypename:   state.ValueType.GetTypeName().String(),
		}
	}

	return h
}

func (h *handler) AsHandler() RequestReplyHandler {
	log.Println("Create RequestReplyHandler")
	for typeName := range h.module {
		log.Printf("> Registering %s\n", typeName)
	}
	return h
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		http.Error(writer, "invalid request method", http.StatusMethodNotAllowed)
	}

	contentType := request.Header.Get("Content-type")
	if contentType != "" && contentType != "application/octet-stream" {
		http.Error(writer, "invalid content type", http.StatusUnsupportedMediaType)
	}

	if request.Body == nil || request.ContentLength == 0 {
		http.Error(writer, "empty request body", http.StatusBadRequest)
	}

	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

	if _, err := buffer.ReadFrom(request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
	}

	response, err := h.Invoke(request.Context(), buffer.Bytes())
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}

	_, _ = writer.Write(response)
}

func (h *handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := protocol.ToFunction{}
	if err := proto.Unmarshal(payload, &toFunction); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ToFunction: %w", err)
	}

	batch := toFunction.GetInvocation()
	self := addressFromInternal(batch.Target)
	function, exists := h.module[self.TypeName]

	if !exists {
		return nil, fmt.Errorf("unknown Function type %s", self.TypeName)
	}

	specs := h.stateSpecs[self.TypeName]
	storage := &AddressScopedStorage{
		mutex:  sync.RWMutex{},
		states: make(map[string]*protocol.TypedValue, len(specs)),
	}

	states := make(map[string]*protocol.FromFunction_PersistedValueSpec, len(specs))
	for k, v := range specs {
		states[k] = v
	}

	for _, state := range batch.State {
		if _, exists := states[state.StateName]; !exists {
			continue
		}

		delete(states, state.StateName)
		storage.states[state.StateName] = state.StateValue
	}

	if len(states) > 0 {
		var missing = make([]*protocol.FromFunction_PersistedValueSpec, len(states))
		for _, spec := range states {
			missing = append(missing, spec)
		}

		fromFunction := protocol.FromFunction{
			Response: &protocol.FromFunction_IncompleteInvocationContext_{
				IncompleteInvocationContext: &protocol.FromFunction_IncompleteInvocationContext{
					MissingValues: missing,
				},
			},
		}

		return proto.Marshal(&fromFunction)
	}

	var outgoing Mailbox
	ctx = context.WithValue(ctx, selfKey, self)

	for _, invocation := range batch.Invocations {
		caller := addressFromInternal(invocation.Caller)
		ctx = context.WithValue(ctx, callerKey, caller)

		msg := Message{
			target:     batch.Target,
			typedValue: invocation.Argument,
		}

		mailbox, err := function.Invoke(ctx, storage, msg)
		if err != nil {
			return nil, fmt.Errorf("failed to execute invocation: %w", err)
		}

		if mailbox != nil {
			outgoing.append(mailbox)
		}
	}

	mutations := make([]*protocol.FromFunction_PersistedValueMutation, len(storage.mutated))
	for name := range storage.mutated {
		typedValue := storage.states[name]

		mutationType := protocol.FromFunction_PersistedValueMutation_MODIFY
		if !typedValue.HasValue {
			mutationType = protocol.FromFunction_PersistedValueMutation_DELETE
		}

		mutation := &protocol.FromFunction_PersistedValueMutation{
			MutationType: mutationType,
			StateName:    name,
			StateValue:   typedValue,
		}

		mutations = append(mutations, mutation)
	}

	fromFunction := protocol.FromFunction{
		Response: &protocol.FromFunction_InvocationResult{
			InvocationResult: &protocol.FromFunction_InvocationResponse{
				StateMutations:     mutations,
				OutgoingMessages:   outgoing.outgoingMessages,
				DelayedInvocations: outgoing.delayedInvocations,
				OutgoingEgresses:   outgoing.outgoingEgresses,
			},
		},
	}

	return proto.Marshal(&fromFunction)
}
