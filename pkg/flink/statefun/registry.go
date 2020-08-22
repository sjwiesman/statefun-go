package statefun

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/errors"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/messages"
	"github.com/valyala/bytebufferpool"
	"log"
	"net/http"
)

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
	// This method provides compliance with AWS Lambda
	// handler.
	Invoke(ctx context.Context, payload []byte) ([]byte, error)

	// Register a StatefulFunction under a FunctionType.
	RegisterFunction(funcType FunctionType, function StatefulFunction)

	// Registers a function pointer as a StatefulFunction under a FunctionType.
	RegisterFunctionPointer(
		funcType FunctionType,
		function func(context.Context, StatefulFunctionRuntime, *any.Any) error)
}

// The statefulFunctionPointer type is an adapter to allow the use of
// ordinary functions as StatefulFunction. If f is a function
// with the appropriate signature, statefulFunctionPointer(f) is a
// Handler that calls f.
type statefulFunctionPointer func(context.Context, StatefulFunctionRuntime, *any.Any) error

func (f statefulFunctionPointer) Invoke(ctx context.Context, rt StatefulFunctionRuntime, msg *any.Any) error {
	return f(ctx, rt, msg)
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

func (functions *functions) RegisterFunctionPointer(
	funcType FunctionType,
	function func(context.Context, StatefulFunctionRuntime, *any.Any) error) {

	log.Printf("registering stateful function %s", funcType.String())
	functions.module[funcType] = statefulFunctionPointer(function)
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
		return
	}

	bytes, err := functions.Invoke(req.Context(), buffer.Bytes())
	if err != nil {
		http.Error(w, err.Error(), errors.ToCode(err))
		log.Print(err)
		return
	}

	_, _ = w.Write(bytes)
}

func (functions functions) Invoke(ctx context.Context, payload []byte) ([]byte, error) {
	toFunction := &messages.ToFunction{}
	if err := proto.Unmarshal(payload, toFunction); err != nil {
		return nil, errors.BadRequest("failed to unmarshal payload %w", err)
	}

	fromFunction, err := executeBatch(functions, ctx, toFunction)
	if err != nil {
		return nil, err
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
		return nil, errors.BadRequest("missing invocations for batch")
	}

	funcType := FunctionType{
		Namespace: invocations.Target.Namespace,
		Type:      invocations.Target.Type,
	}

	function, exists := functions.module[funcType]
	if !exists {
		return nil, errors.BadRequest("%s does not exist", funcType.String())
	}

	runtime, err := newRuntime(invocations.State)
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup runtime for batch")
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
				return nil, errors.Wrap(err, "failed to execute function %s", self.String())
			}
		}
	}

	return runtime.fromFunction()
}
