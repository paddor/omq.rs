package omq

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type Socket struct {
	handle     *nativeSocket
	socketType SocketType
	closed     atomic.Bool
	ops        chan socketOp
	ownerDone  chan struct{}
	closeOnce  sync.Once
}

type socketOp struct {
	fn   func(*nativeSocket) (any, error)
	resp chan socketResult
}

type socketResult struct {
	value any
	err   error
}

func newSocket(handle *nativeSocket, socketType SocketType) *Socket {
	socket := &Socket{
		handle:     handle,
		socketType: socketType,
		ops:        make(chan socketOp, 1024),
		ownerDone:  make(chan struct{}),
	}
	go socket.ownerLoop()
	return socket
}

func (s *Socket) ownerLoop() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(s.ownerDone)

	handle := s.handle
	for op := range s.ops {
		value, err := op.fn(handle)
		op.resp <- socketResult{value: value, err: err}
	}
	if handle != nil {
		socketFreeNative(handle)
	}
}

func (s *Socket) call(ctx context.Context, allowClosed bool, fn func(*nativeSocket) (any, error)) (value any, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return nil, ErrClosed
	}
	if !allowClosed && s.closed.Load() {
		return nil, ErrClosed
	}
	defer func() {
		if recover() != nil {
			value = nil
			err = ErrClosed
		}
	}()

	resp := make(chan socketResult, 1)
	op := socketOp{fn: fn, resp: resp}
	select {
	case s.ops <- op:
	case <-s.ownerDone:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, errFromContext(ctx)
	}
	select {
	case result := <-resp:
		return result.value, result.err
	case <-s.ownerDone:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, errFromContext(ctx)
	}
}

func (s *Socket) Bind(endpoint string) (string, error) {
	value, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return socketBindNative(handle, endpoint)
	})
	if err != nil {
		return "", err
	}
	keepAlive(s)
	return value.(string), nil
}

func (s *Socket) Connect(endpoint string) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketConnectNative(handle, endpoint)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Unbind(endpoint string) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketUnbindNative(handle, endpoint)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Disconnect(endpoint string) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketDisconnectNative(handle, endpoint)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Send(ctx context.Context, msg Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return err
		}
		err := s.TrySend(msg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrAgain) {
			return err
		}
		timer := time.NewTimer(retryDelay(i))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errFromContext(ctx)
		case <-timer.C:
		}
	}
}

func (s *Socket) TrySend(msg Message) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketMessageSendNative(handle, msg)
	})
	keepAlive(s)
	return err
}

func (s *Socket) trySendBatch(messages []Message) (int, error) {
	value, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return socketMessagesTrySendNative(handle, messages)
	})
	if err != nil {
		return 0, err
	}
	keepAlive(s)
	return value.(int), nil
}

func (s *Socket) SendTimeout(msg Message, timeout time.Duration) error {
	if timeout == 0 {
		return s.TrySend(msg)
	}
	if timeout < 0 {
		return s.Send(context.Background(), msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Send(ctx, msg)
}

func (s *Socket) Recv(ctx context.Context) (Message, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return Message{}, err
		}
		msg, err := s.TryRecv()
		if err == nil {
			return msg, nil
		}
		if !errors.Is(err, ErrAgain) {
			return Message{}, err
		}
		timer := time.NewTimer(retryDelay(i))
		select {
		case <-ctx.Done():
			timer.Stop()
			return Message{}, errFromContext(ctx)
		case <-timer.C:
		}
	}
}

func (s *Socket) TryRecv() (Message, error) {
	value, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return socketMessageRecvNative(handle)
	})
	if err != nil {
		return Message{}, err
	}
	keepAlive(s)
	return value.(Message), nil
}

func (s *Socket) RecvTimeout(timeout time.Duration) (Message, error) {
	if timeout == 0 {
		return s.TryRecv()
	}
	if timeout < 0 {
		return s.Recv(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Recv(ctx)
}

func (s *Socket) Subscribe(prefix []byte) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketSubscribeNative(handle, prefix)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Unsubscribe(prefix []byte) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketUnsubscribeNative(handle, prefix)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Join(group []byte) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketJoinNative(handle, group)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Leave(group []byte) error {
	_, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return nil, socketLeaveNative(handle, group)
	})
	keepAlive(s)
	return err
}

func (s *Socket) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		_, err = s.call(ctx, true, func(handle *nativeSocket) (any, error) {
			return nil, socketCloseNative(handle, 0, true)
		})
		close(s.ops)
		<-s.ownerDone
		s.handle = nil
	})
	keepAlive(s)
	return err
}

func (s *Socket) CloseLinger(ctx context.Context, linger time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		_, err = s.call(ctx, true, func(handle *nativeSocket) (any, error) {
			return nil, socketCloseNative(handle, linger, false)
		})
		close(s.ops)
		<-s.ownerDone
		s.handle = nil
	})
	keepAlive(s)
	return err
}

func (s *Socket) Type() SocketType {
	if s == nil {
		return 0
	}
	return s.socketType
}

func (s *Socket) free() {
	if s == nil || s.ops == nil {
		return
	}
	_ = s.Close(context.Background())
}

func (s *Socket) noFinalizer() {
	runtime.SetFinalizer(s, nil)
}
