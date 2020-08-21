package statefun

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/types/known/anypb"
	"log"
	"net/http"
)

type StatefulFunctionPointer func(ctx context.Context, runtime StatefulFunctionRuntime, message *any.Any) error

// Keeps a mapping from FunctionType to stateful functions.
// Use this together with an http endpoint to serve
// StatefulFunction implementations.
type FunctionRegistry interface {
	http.Handler

	// Register a StatefulFunction under a FunctionType.
	RegisterFunction(funcType FunctionType, function StatefulFunction)

	// Registers a function pointer as a StatefulFunction under a FunctionType.
	RegisterFunctionPointer(funcType FunctionType, function StatefulFunctionPointer)
}

type pointer struct {
	f func(ctx context.Context, runtime StatefulFunctionRuntime, message *any.Any) error
}

func (pointer *pointer) Invoke(ctx context.Context, runtime StatefulFunctionRuntime, msg *anypb.Any) error {
	return pointer.f(ctx, runtime, msg)
}

type functions struct {
	module map[FunctionType]StatefulFunction
}

func NewFunctionRegistry() FunctionRegistry {
	return &functions{
		module: make(map[FunctionType]StatefulFunction),
	}
}

func (functions *functions) RegisterFunction(funcType FunctionType, function StatefulFunction) {
	log.Printf("registering stateful function %s", funcType.String())
	functions.module[funcType] = function
}

func (functions *functions) RegisterFunctionPointer(funcType FunctionType, function StatefulFunctionPointer) {
	log.Printf("registering stateful function %s", funcType.String())
	functions.module[funcType] = &pointer{
		f: function,
	}
}

func (functions functions) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !validRequest(w, req) {
		return
	}

	toFunction := getPayload(w, req)
	if toFunction == nil {
		return
	}

	response, err := executeBatch(functions, req.Context(), toFunction)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("error processing request %s", proto.MarshalTextString(toFunction))
		log.Print(err)
		return
	}

	bytes, err := proto.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Print(err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes)
}

func getPayload(w http.ResponseWriter, req *http.Request) *messages.ToFunction {
	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

	_, err := buffer.ReadFrom(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	toFunction := &messages.ToFunction{}
	err = proto.Unmarshal(buffer.Bytes(), toFunction)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil
	}

	return toFunction
}

func validRequest(w http.ResponseWriter, req *http.Request) bool {
	if req.Method != "POST" {
		http.Error(w, "invalid request method", http.StatusMethodNotAllowed)
		return false
	}

	contentType := req.Header.Get("Content-type")
	if contentType != "" && contentType != "application/octet-stream" {
		http.Error(w, "invalid content type", http.StatusUnsupportedMediaType)
		return false
	}

	if req.Body == nil || req.ContentLength == 0 {
		http.Error(w, "empty request body", http.StatusBadRequest)
		return false
	}

	return true
}

func fromInternal(address *messages.Address) *Address {
	if address == nil {
		return nil
	}

	return &Address{
		FunctionType: FunctionType{
			Namespace: address.Namespace,
			Type:      address.Type,
		},
		Id: address.Id,
	}
}

func executeBatch(functions functions, ctx context.Context, request *messages.ToFunction) (*messages.FromFunction, error) {
	invocations := request.GetInvocation()
	if invocations == nil {
		return nil, errors.New("missing invocations for batch")
	}

	funcType := FunctionType{
		Namespace: invocations.Target.Namespace,
		Type:      invocations.Target.Type,
	}

	function, exists := functions.module[funcType]
	if !exists {
		return nil, errors.New(funcType.String() + " does not exist")
	}

	runtime := newStateFunIO(invocations.State)

	self := fromInternal(invocations.Target)
	ctx = context.WithValue(ctx, selfKey, self)
	for _, invocation := range invocations.Invocations {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			caller := fromInternal(invocation.Caller)
			ctx = context.WithValue(ctx, callerKey, caller)
			err := function.Invoke(ctx, runtime, (*invocation).Argument)
			if err != nil {
				return nil, fmt.Errorf("failed to execute function %s %w", self.String(), err)
			}
		}
	}

	return runtime.fromFunction()
}
