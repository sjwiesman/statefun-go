package internal

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/sjwiesman/statefun-go/pkg/flink/statefun/internal/errors"
)

func Marshall(value proto.Message) (*any.Any, error) {
	var packedState *any.Any
	switch record := value.(type) {
	case nil:
		packedState = nil
	case *any.Any:
		packedState = record
	default:
		marshalled, err := ptypes.MarshalAny(record)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshall value into any")
		}
		packedState = marshalled
	}
	return packedState, nil
}

func Unmarshall(value *any.Any, receiver proto.Message) error {
	switch unmarshalled := receiver.(type) {
	case nil:
		return errors.New("cannot unmarshall into nil receiver")
	case *any.Any:
		unmarshalled.TypeUrl = value.TypeUrl
		unmarshalled.Value = value.Value
	default:
		err := ptypes.UnmarshalAny(value, receiver)
		if err != nil {
			return err
		}
	}

	return nil
}
