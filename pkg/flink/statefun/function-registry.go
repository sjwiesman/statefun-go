package statefun

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/valyala/bytebufferpool"
	"log"
	"net/http"
)

// Keeps a mapping from FunctionType to stateful functions.
// Use this together with an http endpoint to serve
// StatefulFunction implementations.
type FunctionRegistry interface {
	http.Handler

	// Register a StatefulFunction under a FunctionType.
	RegisterFunction(funcType FunctionType, function StatefulFunction)

	// Registers a function pointer as a StatefulFunction under a FunctionType.
	RegisterFunctionPointer(funcType FunctionType, function func(ctx StatefulFunctionIO, message *any.Any) error)

	// Executes a batch request from the runtime.
	Process(request *ToFunction) (*FromFunction, error)
}

type pointer struct {
	f func(ctx StatefulFunctionIO, message *any.Any) error
}

func (pointer *pointer) Invoke(ctx StatefulFunctionIO, message *any.Any) error {
	return pointer.f(ctx, message)
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
	functions.module[funcType] = function
}

func (functions *functions) RegisterFunctionPointer(funcType FunctionType, function func(ctx StatefulFunctionIO, message *any.Any) error) {
	functions.module[funcType] = &pointer{
		f: function,
	}
}

func (functions functions) Process(request *ToFunction) (*FromFunction, error) {
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

	ctx := newContext(invocations.Target, invocations.State)

	for _, invocation := range invocations.Invocations {
		ctx.caller = invocation.Caller
		err := function.Invoke(&ctx, (*invocation).Argument)
		if err != nil {
			return nil, fmt.Errorf("failed to execute function %s/%s\n%w", ctx.self.Namespace, ctx.self.Type, err)
		}
	}

	return ctx.fromFunction()
}

func (functions functions) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	if req.Body == nil || req.ContentLength == 0 {
		http.Error(w, "Empty request body", http.StatusBadRequest)
		return
	}

	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

	_, err := buffer.ReadFrom(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var request ToFunction
	err = proto.Unmarshal(buffer.Bytes(), &request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response, err := functions.Process(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error processing request %s", request.String())
		log.Print(err)
		return
	}

	bytes, err := proto.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes)
}
