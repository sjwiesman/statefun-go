package statefun

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"statefun-sdk-go/pkg/statefun/internal"
	"statefun-sdk-go/pkg/statefun/internal/protocol"
)

type executor struct {
	ctx         context.Context
	target      *protocol.Address
	function    StatefulFunction
	factory     StorageFactory
	invocations chan *protocol.ToFunction_Invocation
	mailbox     chan Envelope
	failure     chan error
	processed   bool
}

func newExecutor(
	ctx context.Context,
	batch *protocol.ToFunction_InvocationBatchRequest,
	function StatefulFunction,
	specs map[string]*protocol.FromFunction_PersistedValueSpec) *executor {
	invocations := make(chan *protocol.ToFunction_Invocation, len(batch.Invocations))

	for _, invocation := range batch.Invocations {
		invocations <- invocation
	}

	close(invocations)

	mailbox := make(chan Envelope)

	// assignment to this variable is required,
	// context stores the channel as interface{}
	// and will not retain the correct type information
	// leading to a runtime cast exception
	var send chan<- Envelope = mailbox
	ctx = context.WithValue(ctx, internal.MailboxKey, send)

	return &executor{
		ctx:         ctx,
		target:      batch.Target,
		function:    function,
		factory:     NewStorageFactory(batch, specs),
		invocations: invocations,
		mailbox:     mailbox,
		failure:     make(chan error),
	}
}

func (e *executor) run() (*protocol.FromFunction, error) {
	if e.processed {
		log.Fatalf("an executor can only be run once")
	}

	e.processed = true

	if missing := e.factory.GetMissingSpecs(); missing != nil {
		return &protocol.FromFunction{
			Response: &protocol.FromFunction_IncompleteInvocationContext_{
				IncompleteInvocationContext: &protocol.FromFunction_IncompleteInvocationContext{
					MissingValues: missing,
				},
			},
		}, nil
	}

	storage := e.factory.GetStorage()

	go e.executeBatch(storage)
	defer close(e.failure)

	result := &protocol.FromFunction_InvocationResponse{}

	for {
		select {
		case <-e.ctx.Done():
			return nil, e.ctx.Err()
		case err := <-e.failure:
			return nil, err
		case mail, open := <-e.mailbox:
			if !open {
				result.StateMutations = storage.getStateMutations()
				fromFunction := &protocol.FromFunction{
					Response: &protocol.FromFunction_InvocationResult{
						InvocationResult: result,
					},
				}
				return fromFunction, e.finalError()
			}

			switch mail := mail.(type) {
			case MessageBuilder:
				msg, err := mail.ToMessage()
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

func (e *executor) executeBatch(storage *storage) {
	defer close(e.mailbox)
	defer func() {
		if r := recover(); r != nil {
			switch r := r.(type) {
			case error:
				e.broadcastError(r)
			default:
				log.Fatal(r)
			}
		}
	}()

	for {
		select {
		case <-e.ctx.Done():
			return
		case invocation, open := <-e.invocations:
			if !open {
				return
			}

			ctx := e.ctx
			if invocation.Caller != nil {
				caller := addressFromInternal(invocation.Caller)
				ctx = context.WithValue(e.ctx, internal.CallerKey, caller)
			}

			msg := Message{
				target:     e.target,
				typedValue: invocation.Argument,
			}

			err := e.function.Invoke(ctx, storage, msg)

			if err != nil {
				e.broadcastError(err)
				return
			}
		}
	}
}

func (e *executor) broadcastError(inner error) {
	e.failure <- fmt.Errorf("failed to execute invocation for %s/%s: %w", e.target.Namespace, e.target.Type, inner)
}

func (e *executor) finalError() error {
	for {
		select {
		case err := <-e.failure:
			return err
		default:
			return nil
		}
	}
}
