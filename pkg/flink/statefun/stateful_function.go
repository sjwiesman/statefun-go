package statefun

import (
	any "google.golang.org/protobuf/types/known/anypb"
)

type StatefulFunction interface {
	Invoke(message *any.Any, ctx *Context) error
}
