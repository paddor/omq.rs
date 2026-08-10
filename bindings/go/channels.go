package omq

import (
	"context"
	"errors"
	"sync"
)

type ChannelOptions struct {
	Capacity      int
	OverrunPolicy OverrunPolicy
}

type SocketChannels struct {
	Rx     <-chan Message
	Tx     chan<- Message
	Events <-chan MonitorEvent
	Errors <-chan error

	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Socket) Channels(ctx context.Context, opts ChannelOptions) (*SocketChannels, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	policy := opts.OverrunPolicy
	if policy == OverrunBlock {
		policy = s.overrun
	}
	capacity := opts.Capacity
	if capacity <= 0 {
		capacity = 1024
	}
	ctx, cancel := context.WithCancel(ctx)
	errorsCh := make(chan error, capacity)
	eventsCh := make(chan MonitorEvent, capacity)
	done := make(chan struct{})
	var wg sync.WaitGroup

	var rx chan Message
	var tx chan Message

	if s.socketType.canRecv() {
		rx = make(chan Message, capacity)
		wg.Add(1)
		go func() {
			defer close(rx)
			defer wg.Done()
			for {
				msg, err := s.Recv(ctx)
				if err != nil {
					if !errors.Is(err, ErrCanceled) && !errors.Is(err, ErrClosed) {
						reportError(ctx, errorsCh, err)
					}
					return
				}
				ok, err := sendRx(ctx, rx, msg, policy)
				if err != nil {
					reportError(ctx, errorsCh, err)
				}
				if !ok {
					return
				}
			}
		}()
	}

	if s.socketType.canSend() {
		tx = make(chan Message, capacity)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-tx:
					if !ok {
						return
					}
					if !sendChannelBatch(ctx, s, tx, msg, errorsCh) {
						return
					}
				}
			}
		}()
	}

	monitor, err := s.Monitor()
	if err == nil {
		wg.Add(1)
		go func() {
			defer monitor.Close()
			defer close(eventsCh)
			defer wg.Done()
			for {
				event, err := monitor.Recv(ctx)
				if err != nil {
					if !errors.Is(err, ErrCanceled) && !errors.Is(err, ErrClosed) {
						reportError(ctx, errorsCh, err)
					}
					return
				}
				select {
				case eventsCh <- event:
				case <-ctx.Done():
					return
				}
			}
		}()
	} else {
		close(eventsCh)
	}

	go func() {
		wg.Wait()
		close(errorsCh)
		close(done)
	}()

	return &SocketChannels{
		Rx:     rx,
		Tx:     tx,
		Events: eventsCh,
		Errors: errorsCh,
		cancel: cancel,
		done:   done,
	}, nil
}

func (c *SocketChannels) Close() {
	if c == nil {
		return
	}
	c.cancel()
	<-c.done
}

func reportError(ctx context.Context, errorsCh chan<- error, err error) {
	select {
	case errorsCh <- err:
	case <-ctx.Done():
	default:
	}
}

func sendRx(ctx context.Context, rx chan Message, msg Message, policy OverrunPolicy) (bool, error) {
	switch policy {
	case OverrunDropNewest:
		select {
		case rx <- msg:
		default:
		}
		return true, nil
	case OverrunDropOldest:
		select {
		case rx <- msg:
			return true, nil
		default:
		}
		select {
		case <-rx:
		default:
		}
		select {
		case rx <- msg:
		case <-ctx.Done():
			return false, nil
		}
		return true, nil
	case OverrunReturnError:
		select {
		case rx <- msg:
			return true, nil
		default:
			return false, &Error{Err: "receive channel overrun"}
		}
	default:
		select {
		case rx <- msg:
			return true, nil
		case <-ctx.Done():
			return false, nil
		}
	}
}

func sendChannelBatch(ctx context.Context, socket *Socket, tx <-chan Message, first Message, errorsCh chan<- error) bool {
	batch := make([]Message, 0, 64)
	batch = append(batch, first)
drain:
	for len(batch) < cap(batch) {
		select {
		case msg, ok := <-tx:
			if !ok {
				break drain
			}
			batch = append(batch, msg)
		default:
			break drain
		}
	}

	for len(batch) > 0 {
		sent, err := socket.trySendBatch(batch)
		if sent > 0 {
			batch = batch[sent:]
			continue
		}
		if err == nil {
			return true
		}
		if errors.Is(err, ErrAgain) {
			if err := socket.Send(ctx, batch[0]); err != nil {
				if !errors.Is(err, ErrCanceled) && !errors.Is(err, ErrClosed) {
					reportError(ctx, errorsCh, err)
				}
				return false
			}
			batch = batch[1:]
			continue
		}
		if !errors.Is(err, ErrCanceled) && !errors.Is(err, ErrClosed) {
			reportError(ctx, errorsCh, err)
		}
		return false
	}
	return true
}
