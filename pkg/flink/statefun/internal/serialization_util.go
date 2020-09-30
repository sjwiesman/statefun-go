package internal

import (
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func Marshall(value proto.Message) (*anypb.Any, error)  {
	var packedState *anypb.Any
	switch record := value.(type) {
	case nil:
		packedState = nil
	case *anypb.Any:
		packedState = record
	default:
		packedState = &anypb.Any{}
		if err := packedState.MarshalFrom(record); err != nil {
			return nil, errors.Wrap(err, "failed to marshall value into any")
		}
	}
	return packedState, nil
}

func Unmarshall(value *anypb.Any, receiver proto.Message) error {
	switch unmarshalled := receiver.(type) {
	case nil:
		return errors.New("cannot unmarshall into nil receiver")
	case *anypb.Any:
		unmarshalled.TypeUrl = value.TypeUrl
		unmarshalled.Value = value.Value
	default:
		if err := value.UnmarshalTo(receiver); err != nil {
			return err
		}
	}

	return nil
}
