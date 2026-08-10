package omq

import (
	"context"
	"errors"
	"time"
)

type ReceiveEvent struct {
	Socket  *Socket
	Message Message
}

func TryReceiveAny(sockets ...*Socket) (ReceiveEvent, error) {
	if err := validateReceiveAnySockets(sockets); err != nil {
		return ReceiveEvent{}, err
	}
	for _, socket := range sockets {
		msg, err := socket.TryRecv()
		if err == nil {
			return ReceiveEvent{Socket: socket, Message: msg}, nil
		}
		if !errors.Is(err, ErrAgain) {
			return ReceiveEvent{}, err
		}
	}
	return ReceiveEvent{}, ErrAgain
}

func ReceiveAny(ctx context.Context, sockets ...*Socket) (ReceiveEvent, error) {
	if err := validateReceiveAnySockets(sockets); err != nil {
		return ReceiveEvent{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return ReceiveEvent{}, err
		}
		event, err := TryReceiveAny(sockets...)
		if err == nil {
			return event, nil
		}
		if !errors.Is(err, ErrAgain) {
			return ReceiveEvent{}, err
		}
		if err := waitRetry(ctx, i); err != nil {
			return ReceiveEvent{}, err
		}
	}
}

func ReceiveAnyTimeout(timeout time.Duration, sockets ...*Socket) (ReceiveEvent, error) {
	if timeout == 0 {
		return TryReceiveAny(sockets...)
	}
	if timeout < 0 {
		return ReceiveAny(context.Background(), sockets...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return ReceiveAny(ctx, sockets...)
}

func validateReceiveAnySockets(sockets []*Socket) error {
	if len(sockets) == 0 {
		return &ConfigError{Err: "receive-any requires at least one socket"}
	}
	seen := make(map[*Socket]struct{}, len(sockets))
	for _, socket := range sockets {
		if socket == nil {
			return &ConfigError{Err: "receive-any socket is nil"}
		}
		if _, ok := seen[socket]; ok {
			return &ConfigError{Err: "receive-any sockets must be unique"}
		}
		seen[socket] = struct{}{}
	}
	return nil
}
