package statefun

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"statefun-sdk-go/pkg/statefun/internal"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
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
		module:     map[TypeName]StatefulFunction{},
		stateSpecs: map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec{},
	}
}

type handler struct {
	module     map[TypeName]StatefulFunction
	stateSpecs map[TypeName]map[string]*protocol.FromFunction_PersistedValueSpec
}

func (h *handler) WithSpec(spec StatefulFunctionSpec) StatefulFunctions {
	h.module[spec.FunctionType] = spec.Function
	h.stateSpecs[spec.FunctionType] = make(map[string]*protocol.FromFunction_PersistedValueSpec, len(spec.States))

	for _, state := range spec.States {
		expiration := &protocol.FromFunction_ExpirationSpec{}
		if state.Expiration == nil {
			expiration.Mode = protocol.FromFunction_ExpirationSpec_NONE
		} else {
			switch state.Expiration.expirationType {
			case expireAfterWrite:
				expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_WRITE
				expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
			case expireAfterCall:
				expiration.Mode = protocol.FromFunction_ExpirationSpec_AFTER_INVOKE
				expiration.ExpireAfterMillis = state.Expiration.duration.Milliseconds()
			}
		}

		h.stateSpecs[spec.FunctionType][state.Name] = &protocol.FromFunction_PersistedValueSpec{
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

	batch := toFunction.GetInvocation()
	self := addressFromInternal(batch.Target)
	function, exists := h.module[self.TypeName]

	if !exists {
		return nil, fmt.Errorf("unknown Function type %s", self.TypeName)
	}

	ctx = context.WithValue(ctx, internal.SelfKey, self)
	executor := newExecutor(ctx, batch, function, h.stateSpecs[self.TypeName])
	fromFunction, err := executor.run()

	if err != nil {
		return nil, err
	}

	return proto.Marshal(fromFunction)
}
