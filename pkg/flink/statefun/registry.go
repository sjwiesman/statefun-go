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

// Keeps a mapping from FunctionType to stateful functions
// and serves them to the Flink runtime.
//
// HTTP Endpoint
//
//    import "net/http"
//
//	  func main() {
//    	registry := NewFunctionRegistry()
//		registry.RegisterFunction(greeterType, GreeterFunction{})
//
//	  	http.Handle("/service", registry)
//	  	_ = http.ListenAndServe(":8000", nil)
//	  }
//
// AWS Lambda
//
//    import "github.com/aws/aws-lambda"
//
//	  func main() {
//    	registry := NewFunctionRegistry()
//		registry.RegisterFunction(greeterType, GreeterFunction{})
//
//		lambda.StartHandler(registry)
//	  }
type FunctionRegistry interface {
	// Handler for processing runtime messages from
	// an http endpoint
	http.Handler

	// Handler for processing arbitrary payloads.
	Invoke(ctx context.Context, payload []byte) ([]byte, error)

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

	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)

	_, err := buffer.ReadFrom(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	bytes, err := functions.Invoke(req.Context(), buffer.Bytes())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Print(err)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(bytes)
}

func (functions functions) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := &messages.ToFunction{}
	if err := proto.Unmarshal(payload, toFunction); err != nil {
		return nil, fmt.Errorf("failed to unmarhsal payload %w", err)
	}

	fromFunction, err := executeBatch(functions, ctx, toFunction)
	if err != nil {
		return nil, fmt.Errorf("error processing request %s %w", proto.MarshalTextString(toFunction), err)
	}

	return proto.Marshal(fromFunction)
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

	runtime, err := newRuntime(invocations.State)
	if err != nil {
		return nil, fmt.Errorf("failed to setup runtime for batch %w", err)
	}

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
