package statefun

import (
	"context"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"
	"log"
	"net/http"
	"reflect"
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

	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

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

	storage := &storage{
		mutex:   sync.RWMutex{},
		states:  make(map[string]*protocol.TypedValue, len(h.stateSpecs[self.TypeName])),
		mutated: make(map[string]bool, len(h.stateSpecs[self.TypeName])),
	}

	if bytes, err := h.fillStorage(self.TypeName, batch, storage); err != nil {
		return nil, err
	} else if bytes != nil {
		return bytes, nil
	}

	ctx = context.WithValue(ctx, selfKey, self)
	mailbox := make(chan Envelope)

	// assignment to this variable is required,
	// context stores the channel as interface{}
	// and will not retain the correct type information
	// leading to a runtime cast exception
	var send chan<- Envelope = mailbox
	ctx = context.WithValue(ctx, mailboxKey, send)

	failure := make(chan error)
	defer close(failure)

	invocations := make(chan *protocol.ToFunction_Invocation, len(batch.Invocations))

	go execute(ctx, batch.Target, invocations, function, storage, mailbox, failure, self)

	for _, invocation := range batch.Invocations {
		invocations <- invocation
	}
	close(invocations)

	result := &protocol.FromFunction_InvocationResponse{}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-failure:
			return nil, err
		case mail, open := <-mailbox:
			if !open {
				fromFunction := completeResponse(storage, result)
				return proto.Marshal(fromFunction)
			}

			switch mail := mail.(type) {
			case MessageBuilder:
				msg, err := mail.toMessage()
				if err != nil {
					return nil, err
				}

				if msg.delayMs > 0 {
					invocation := &protocol.FromFunction_DelayedInvocation{
						Target:    msg.target,
						Argument:  msg.typedValue,
						DelayInMs: msg.delayMs,
					}

					result.DelayedInvocations = append(result.DelayedInvocations, invocation)
				} else {
					invocation := &protocol.FromFunction_Invocation{
						Target:   msg.target,
						Argument: msg.typedValue,
					}

					result.OutgoingMessages = append(result.OutgoingMessages, invocation)
				}
			case EgressBuilder:
				msg, err := mail.toEgressMessage()
				if err != nil {
					return nil, err
				}

				result.OutgoingEgresses = append(result.OutgoingEgresses, msg)
			default:
				log.Fatalf("unknown Envelope type %s", reflect.TypeOf(mail))
			}
		}
	}
}

func (h *handler) fillStorage(typeName TypeName, batch *protocol.ToFunction_InvocationBatchRequest, storage *storage) ([]byte, error) {
	specs := h.stateSpecs[typeName]
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
		var missing = make([]*protocol.FromFunction_PersistedValueSpec, 0, len(states))
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
	return nil, nil
}

func completeResponse(storage *storage, result *protocol.FromFunction_InvocationResponse) *protocol.FromFunction {
	mutations := make([]*protocol.FromFunction_PersistedValueMutation, 0, len(storage.mutated))
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

	result.StateMutations = mutations
	fromFunction := &protocol.FromFunction{
		Response: &protocol.FromFunction_InvocationResult{
			InvocationResult: result,
		},
	}
	return fromFunction
}

func execute(
	ctx context.Context,
	target *protocol.Address,
	invocations <-chan *protocol.ToFunction_Invocation,
	function StatefulFunction,
	storage *storage,
	mailbox chan Envelope,
	failure chan<- error,
	self *Address) {

	defer close(mailbox)
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				err := fmt.Errorf("failed to execute invocation for %s: %w", self.TypeName, r)
				failure <- err
			default:
				log.Fatal(r)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case invocation, open := <-invocations:
			if !open {
				return
			}
			if invocation.Caller != nil {
				caller := addressFromInternal(invocation.Caller)
				ctx = context.WithValue(ctx, callerKey, caller)
			}

			msg := Message{
				target:     target,
				typedValue: invocation.Argument,
			}

			err := function.Invoke(ctx, storage, msg)

			if err != nil {
				failure <- fmt.Errorf("failed to execute invocation for %s: %w", self.TypeName, err)
				return
			}
		}
	}
}
