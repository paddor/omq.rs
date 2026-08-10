package omq

import (
	"bytes"
	"context"
	"errors"
)

type ProxyOptions struct {
	Capture   *Socket
	Control   *Socket
	BurstSize int
}

func Proxy(ctx context.Context, frontend, backend *Socket, opts ProxyOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if frontend == nil || backend == nil {
		return &ConfigError{Err: "proxy sockets must not be nil"}
	}
	burst := opts.BurstSize
	if burst <= 0 {
		burst = 64
	}
	paused := false
	for {
		if err := errFromContext(ctx); err != nil {
			return err
		}
		if opts.Control != nil {
			done, err := proxyHandleControl(ctx, opts.Control, &paused)
			if done || err != nil {
				return err
			}
		}
		if paused {
			if opts.Control == nil {
				return &ConfigError{Err: "proxy paused without control socket"}
			}
			event, err := ReceiveAny(ctx, opts.Control)
			if err != nil {
				return err
			}
			done, err := proxyApplyControl(ctx, opts.Control, event.Message, &paused)
			if done || err != nil {
				return err
			}
			continue
		}
		var event ReceiveEvent
		var err error
		if opts.Control != nil {
			event, err = ReceiveAny(ctx, opts.Control, frontend, backend)
		} else {
			event, err = ReceiveAny(ctx, frontend, backend)
		}
		if err != nil {
			return err
		}
		if event.Socket == opts.Control {
			done, err := proxyApplyControl(ctx, opts.Control, event.Message, &paused)
			if done || err != nil {
				return err
			}
		} else if event.Socket == frontend {
			if err := proxyForwardBurst(ctx, frontend, backend, opts.Capture, event.Message, burst); err != nil {
				return err
			}
		} else if err := proxyForwardBurst(ctx, backend, frontend, opts.Capture, event.Message, burst); err != nil {
			return err
		}
	}
}

func proxyForwardBurst(
	ctx context.Context,
	source *Socket,
	target *Socket,
	capture *Socket,
	first Message,
	burst int,
) error {
	if err := proxyForward(ctx, target, capture, first); err != nil {
		return err
	}
	for i := 1; i < burst; i++ {
		msg, err := source.TryRecv()
		if errors.Is(err, ErrAgain) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := proxyForward(ctx, target, capture, msg); err != nil {
			return err
		}
	}
	return nil
}

func proxyForward(ctx context.Context, target *Socket, capture *Socket, msg Message) error {
	if capture != nil {
		_ = capture.TrySend(msg)
	}
	return target.Send(ctx, msg)
}

func proxyHandleControl(ctx context.Context, control *Socket, paused *bool) (bool, error) {
	msg, err := control.TryRecv()
	if errors.Is(err, ErrAgain) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return proxyApplyControl(ctx, control, msg, paused)
}

func proxyApplyControl(ctx context.Context, control *Socket, msg Message, paused *bool) (bool, error) {
	command := msg.Bytes()
	switch {
	case bytes.Equal(command, []byte("PAUSE")):
		*paused = true
	case bytes.Equal(command, []byte("RESUME")):
		*paused = false
	case bytes.Equal(command, []byte("TERMINATE")), bytes.Equal(command, []byte("KILL")):
		return true, nil
	}
	if control.socketType == Rep {
		if err := control.Send(ctx, Bytes(nil)); err != nil {
			return false, err
		}
	}
	return false, nil
}
