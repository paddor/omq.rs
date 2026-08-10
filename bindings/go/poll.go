package omq

import (
	"context"
	"errors"
	"time"
)

const receiveAnyContextPoll = time.Millisecond

// ReceiveEvent identifies the socket that produced a message.
type ReceiveEvent struct {
	// Socket is the socket that produced Message.
	Socket *Socket
	// Message is the received message.
	Message Message
}

// TryReceiveAny receives from the first ready socket without waiting.
func TryReceiveAny(sockets ...*Socket) (ReceiveEvent, error) {
	if err := validateReceiveAnySockets(sockets); err != nil {
		return ReceiveEvent{}, err
	}
	return receiveAnyNative(sockets, 0)
}

// ReceiveAny receives from any supplied socket until ctx is done.
func ReceiveAny(ctx context.Context, sockets ...*Socket) (ReceiveEvent, error) {
	if err := validateReceiveAnySockets(sockets); err != nil {
		return ReceiveEvent{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := errFromContext(ctx); err != nil {
			return ReceiveEvent{}, err
		}
		event, err := receiveAnyNative(sockets, receiveAnyTimeoutMillis(ctx))
		if err == nil {
			return event, nil
		}
		if !errors.Is(err, ErrAgain) && !errors.Is(err, ErrTimeout) {
			return ReceiveEvent{}, err
		}
	}
}

// ReceiveAnyTimeout receives from any socket with libzmq-style timeout semantics.
func ReceiveAnyTimeout(timeout time.Duration, sockets ...*Socket) (ReceiveEvent, error) {
	if timeout == 0 {
		return TryReceiveAny(sockets...)
	}
	if timeout < 0 {
		if err := validateReceiveAnySockets(sockets); err != nil {
			return ReceiveEvent{}, err
		}
		return receiveAnyNative(sockets, -1)
	}
	if err := validateReceiveAnySockets(sockets); err != nil {
		return ReceiveEvent{}, err
	}
	return receiveAnyNative(sockets, durationMillis(timeout))
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

func receiveAnyTimeoutMillis(ctx context.Context) int64 {
	timeout := receiveAnyContextPoll
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	return durationMillis(timeout)
}

// Poller receives from a stable set of sockets.
type Poller struct {
	sockets []*Socket
}

// NewPoller creates a poller for distinct sockets.
func NewPoller(sockets ...*Socket) (*Poller, error) {
	if err := validateReceiveAnySockets(sockets); err != nil {
		return nil, err
	}
	return &Poller{sockets: append([]*Socket(nil), sockets...)}, nil
}

// Sockets returns a copy of this poller's sockets.
func (p *Poller) Sockets() []*Socket {
	if p == nil {
		return nil
	}
	return append([]*Socket(nil), p.sockets...)
}

// TryRecv receives from the first ready poller socket without waiting.
func (p *Poller) TryRecv() (ReceiveEvent, error) {
	if p == nil {
		return ReceiveEvent{}, &ConfigError{Err: "poller is nil"}
	}
	return TryReceiveAny(p.sockets...)
}

// Recv receives from any poller socket until ctx is done.
func (p *Poller) Recv(ctx context.Context) (ReceiveEvent, error) {
	if p == nil {
		return ReceiveEvent{}, &ConfigError{Err: "poller is nil"}
	}
	return ReceiveAny(ctx, p.sockets...)
}

// RecvTimeout receives from any poller socket with timeout semantics.
func (p *Poller) RecvTimeout(timeout time.Duration) (ReceiveEvent, error) {
	if p == nil {
		return ReceiveEvent{}, &ConfigError{Err: "poller is nil"}
	}
	return ReceiveAnyTimeout(timeout, p.sockets...)
}
