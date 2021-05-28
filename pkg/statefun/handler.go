package statefun

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

// A registry for multiple StatefulFunction's. A RequestReplyHandler
// can be created from the registry that understands how to dispatch
// invocation requests to the registered functions as well as encode
// side-effects (e.g., sending messages to other functions or updating
// values in storage) as the response.
type StatefulFunctions interface {

	// Registers a StatefulFunctionSpec, which will be
	// used to build the runtime function.
	WithSpec(spec StatefulFunctionSpec) error

	// Creates a RequestReplyHandler from the registered
	// function specs.
	AsHandler() RequestReplyHandler
}

// The RequestReplyHandler processes messages
// from the runtime, invokes functions, and encodes
// side effects. The handler implements http.Handler
// so it can easily be embedded in standard Go server
// frameworks.
type RequestReplyHandler interface {
	http.Handler

	// This method provides compliance with AWS Lambda handler
	Invoke(ctx context.Context, payload []byte) ([]byte, error)
}

// Creates a new StatefulFunctions registry.
func StatefulFunctionsBuilder() StatefulFunctions {
	return &handler{
		module:     map[TypeName]StatefulFunction{},
		stateSpecs: map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec{},
	}
}

type handler struct {
	module     map[TypeName]StatefulFunction
	stateSpecs map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec
}

func (h *handler) WithSpec(spec StatefulFunctionSpec) error {
	if _, exists := h.module[spec.FunctionType]; exists {
		return fmt.Errorf("failed to register Stateful Function %s, there is already a spec registered under that tpe", spec.FunctionType)
	}

	if spec.Function == nil {
		return fmt.Errorf("failed to register Stateful Function %s, the Function instance cannot be nil", spec.FunctionType)
	}

	h.module[spec.FunctionType] = spec.Function
	h.stateSpecs[spec.FunctionType] = make(map[string]*protocol.FromFunction_PersistedValueSpec, len(spec.States))

	for _, state := range spec.States {
		if err := validateValueSpec(state); err != nil {
			return fmt.Errorf("failed to register Stateful Function %s: %w", spec.FunctionType, err)
		}

		expiration := &protocol.FromFunction_ExpirationSpec{}
		switch state.Expiration.expirationType {
		case none:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_NONE
		case expireAfterWrite:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_WRITE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		case expireAfterCall:
			expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_INVOKE
			expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
		}

		h.stateSpecs[spec.FunctionType][state.Name] = &protocol.FromFunction_PersistedValueSpec{
			StateName:      state.Name,
			ExpirationSpec: expiration,
			TypeTypename:   state.ValueType.GetTypeName().String(),
		}
	}

	return nil
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
		return
	}

	contentType := request.Header.Get("Content-type")
	if contentType != "" && contentType != "application/octet-stream" {
		http.Error(writer, "invalid content type", http.StatusUnsupportedMediaType)
		return
	}

	if request.Body == nil || request.ContentLength == 0 {
		http.Error(writer, "empty request body", http.StatusBadRequest)
		return
	}

	buffer := bytes.Buffer{}
	if _, err := buffer.ReadFrom(request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := h.Invoke(request.Context(), buffer.Bytes())
	if err != nil {
		log.Printf(err.Error())
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = writer.Write(response)
}

func (h *handler) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := protocol.ToFunction{}
	if err := proto.Unmarshal(payload, &toFunction); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ToFunction: %w", err)
	}

	fromFunction, err := h.invoke(ctx, &toFunction)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(fromFunction)
}

func (h *handler) invoke(ctx context.Context, toFunction *protocol.ToFunction) (*protocol.FromFunction, error) {
	batch := toFunction.GetInvocation()
	self := addressFromInternal(batch.Target)
	function, exists := h.module[self.FunctionType]

	if !exists {
		return nil, fmt.Errorf("unknown function type %s", self.FunctionType)
	}

	ctx = setSelf(ctx, batch.Target)
	executor := newExecutor(ctx, batch, function, h.stateSpecs[self.FunctionType])
	fromFunction, err := executor.run()

	if err != nil {
		return nil, err
	}
	return fromFunction, nil
}
