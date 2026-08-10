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
	owner      *Context
	ringSize   int
	overrun    OverrunPolicy
	closed     atomic.Bool
	ops        chan socketOp
	ownerDone  chan struct{}
	closeOnce  sync.Once
}

type socketOpKind uint8

const (
	socketOpFunc socketOpKind = iota
	socketOpBind
	socketOpConnect
	socketOpUnbind
	socketOpDisconnect
	socketOpSend
	socketOpSendBatch
	socketOpRecv
	socketOpRecvInto
	socketOpSubscribe
	socketOpUnsubscribe
	socketOpJoin
	socketOpLeave
	socketOpClose
	socketOpRun
)

type socketOp struct {
	kind          socketOpKind
	fn            func(*nativeSocket) (any, error)
	endpoint      string
	msg           Message
	messages      []Message
	buffer        []byte
	data          []byte
	linger        time.Duration
	useConfigured bool
	ctx           context.Context
	run           func(*BoundSocket) error
	socketType    SocketType
	ringSize      int
	call          *socketCall
}

type socketCall struct {
	resp chan socketResult
}

type socketResult struct {
	value   any
	message Message
	text    string
	count   int
	monitor *nativeMonitor
	err     error
}

func newSocket(
	handle *nativeSocket,
	socketType SocketType,
	owner *Context,
	ringSize int,
	overrun OverrunPolicy,
) *Socket {
	socket := &Socket{
		handle:     handle,
		socketType: socketType,
		owner:      owner,
		ringSize:   ringSize,
		overrun:    overrun,
		ops:        make(chan socketOp, 1024),
		ownerDone:  make(chan struct{}),
	}
	go socket.ownerLoop()
	return socket
}

var socketCallPool = sync.Pool{
	New: func() any {
		return &socketCall{resp: make(chan socketResult, 1)}
	},
}

func (s *Socket) ownerLoop() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(s.ownerDone)

	handle := s.handle
	for op := range s.ops {
		op.call.resp <- runSocketOp(handle, op)
	}
	if handle != nil {
		socketFreeNative(handle)
	}
}

func runSocketOp(handle *nativeSocket, op socketOp) socketResult {
	switch op.kind {
	case socketOpFunc:
		value, err := op.fn(handle)
		return socketResult{value: value, err: err}
	case socketOpBind:
		text, err := socketBindNative(handle, op.endpoint)
		return socketResult{text: text, err: err}
	case socketOpConnect:
		return socketResult{err: socketConnectNative(handle, op.endpoint)}
	case socketOpUnbind:
		return socketResult{err: socketUnbindNative(handle, op.endpoint)}
	case socketOpDisconnect:
		return socketResult{err: socketDisconnectNative(handle, op.endpoint)}
	case socketOpSend:
		return socketResult{err: socketMessageSendNative(handle, op.msg)}
	case socketOpSendBatch:
		count, err := socketMessagesTrySendNative(handle, op.messages)
		return socketResult{count: count, err: err}
	case socketOpRecv:
		msg, err := socketMessageRecvNative(handle)
		return socketResult{message: msg, err: err}
	case socketOpRecvInto:
		count, err := socketMessageRecvIntoNative(handle, op.buffer)
		return socketResult{count: count, err: err}
	case socketOpSubscribe:
		return socketResult{err: socketSubscribeNative(handle, op.data)}
	case socketOpUnsubscribe:
		return socketResult{err: socketUnsubscribeNative(handle, op.data)}
	case socketOpJoin:
		return socketResult{err: socketJoinNative(handle, op.data)}
	case socketOpLeave:
		return socketResult{err: socketLeaveNative(handle, op.data)}
	case socketOpClose:
		return socketResult{err: socketCloseNative(handle, op.linger, op.useConfigured)}
	case socketOpRun:
		ctx := op.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		bound := &BoundSocket{
			handle:     handle,
			socketType: op.socketType,
			ringSize:   op.ringSize,
			ctx:        ctx,
		}
		err := op.run(bound)
		closeErr := bound.close()
		if err != nil {
			return socketResult{err: err}
		}
		return socketResult{err: closeErr}
	default:
		return socketResult{err: &ConfigError{Err: "unknown socket op"}}
	}
}

func (s *Socket) call(ctx context.Context, allowClosed bool, fn func(*nativeSocket) (any, error)) (value any, err error) {
	result, err := s.do(ctx, allowClosed, socketOp{kind: socketOpFunc, fn: fn})
	if err != nil {
		return nil, err
	}
	return result.value, result.err
}

func (s *Socket) do(ctx context.Context, allowClosed bool, op socketOp) (result socketResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return socketResult{}, ErrClosed
	}
	if !allowClosed && s.closed.Load() {
		return socketResult{}, ErrClosed
	}
	defer func() {
		if recover() != nil {
			result = socketResult{}
			err = ErrClosed
		}
	}()

	call := socketCallPool.Get().(*socketCall)
	op.call = call
	putCall := true
	defer func() {
		if putCall {
			socketCallPool.Put(call)
		}
	}()
	select {
	case s.ops <- op:
	case <-s.ownerDone:
		return socketResult{}, ErrClosed
	case <-ctx.Done():
		return socketResult{}, errFromContext(ctx)
	}
	select {
	case result := <-call.resp:
		return result, result.err
	case <-s.ownerDone:
		putCall = false
		return socketResult{}, ErrClosed
	case <-ctx.Done():
		putCall = false
		return socketResult{}, errFromContext(ctx)
	}
}

func (s *Socket) Bind(endpoint string) (string, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpBind, endpoint: endpoint})
	if err != nil {
		return "", err
	}
	keepAlive(s)
	return result.text, nil
}

func (s *Socket) Connect(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpConnect, endpoint: endpoint})
	keepAlive(s)
	return err
}

func (s *Socket) Unbind(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpUnbind, endpoint: endpoint})
	keepAlive(s)
	return err
}

func (s *Socket) Disconnect(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpDisconnect, endpoint: endpoint})
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
		if err := waitRetry(ctx, i); err != nil {
			return err
		}
	}
}

func (s *Socket) TrySend(msg Message) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpSend, msg: msg})
	keepAlive(s)
	return err
}

func (s *Socket) trySendBatch(messages []Message) (int, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpSendBatch, messages: messages})
	if err != nil {
		return 0, err
	}
	keepAlive(s)
	return result.count, nil
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
		if err := waitRetry(ctx, i); err != nil {
			return Message{}, err
		}
	}
}

func (s *Socket) RecvInto(ctx context.Context, dst []byte) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return 0, err
		}
		n, err := s.TryRecvInto(dst)
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, ErrAgain) {
			return 0, err
		}
		if err := waitRetry(ctx, i); err != nil {
			return 0, err
		}
	}
}

func (s *Socket) TryRecv() (Message, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpRecv})
	if err != nil {
		return Message{}, err
	}
	keepAlive(s)
	return result.message, nil
}

func (s *Socket) TryRecvInto(dst []byte) (int, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpRecvInto, buffer: dst})
	if err != nil {
		return 0, err
	}
	keepAlive(s)
	return result.count, nil
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

func (s *Socket) RecvIntoTimeout(dst []byte, timeout time.Duration) (int, error) {
	if timeout == 0 {
		return s.TryRecvInto(dst)
	}
	if timeout < 0 {
		return s.RecvInto(context.Background(), dst)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.RecvInto(ctx, dst)
}

func (s *Socket) Subscribe(prefix []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpSubscribe, data: prefix})
	keepAlive(s)
	return err
}

func (s *Socket) Unsubscribe(prefix []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpUnsubscribe, data: prefix})
	keepAlive(s)
	return err
}

func (s *Socket) Join(group []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpJoin, data: group})
	keepAlive(s)
	return err
}

func (s *Socket) Leave(group []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpLeave, data: group})
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
		_, err = s.do(ctx, true, socketOp{kind: socketOpClose, useConfigured: true})
		close(s.ops)
		<-s.ownerDone
		s.handle = nil
		if s.owner != nil {
			s.owner.removeSocket(s)
		}
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
		_, err = s.do(ctx, true, socketOp{kind: socketOpClose, linger: linger})
		close(s.ops)
		<-s.ownerDone
		s.handle = nil
		if s.owner != nil {
			s.owner.removeSocket(s)
		}
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

func (s *Socket) Run(ctx context.Context, fn func(*BoundSocket) error) error {
	if fn == nil {
		return &ConfigError{Err: "nil socket run function"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err := s.do(ctx, false, socketOp{
		kind:       socketOpRun,
		ctx:        ctx,
		run:        fn,
		socketType: s.socketType,
		ringSize:   s.ringSize,
	})
	keepAlive(s)
	return err
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

type BoundSocket struct {
	handle     *nativeSocket
	socketType SocketType
	ringSize   int
	ctx        context.Context
	sendRing   *sendRing
	recvRing   *recvRing
}

func (s *BoundSocket) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *BoundSocket) Send(ctx context.Context, msg Message) error {
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
		if err := waitRetry(ctx, i); err != nil {
			return err
		}
	}
}

func (s *BoundSocket) TrySend(msg Message) error {
	handled, err := s.trySendRing(msg)
	if handled {
		return err
	}
	return socketMessageSendNative(s.handle, msg)
}

func (s *BoundSocket) SendBlocking(msg Message) error {
	ctx := s.Context()
	for i := 0; ; i++ {
		handled, err := s.trySendRing(msg)
		if !handled {
			break
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrAgain) {
			return err
		}
		if err := waitRetry(ctx, i); err != nil {
			return err
		}
	}
	return s.Send(ctx, msg)
}

func (s *BoundSocket) RecvInto(ctx context.Context, dst []byte) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return 0, err
		}
		n, err := s.TryRecvInto(dst)
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, ErrAgain) {
			return 0, err
		}
		if err := waitRetry(ctx, i); err != nil {
			return 0, err
		}
	}
}

func (s *BoundSocket) TryRecvInto(dst []byte) (int, error) {
	ring, err := s.ensureRecvRing()
	if err != nil {
		return 0, err
	}
	return ring.tryRecvInto(dst)
}

func (s *BoundSocket) RecvIntoBlocking(dst []byte) (int, error) {
	ctx := s.Context()
	for i := 0; ; i++ {
		n, err := s.TryRecvInto(dst)
		if err == nil {
			return n, nil
		}
		if !errors.Is(err, ErrAgain) {
			return 0, err
		}
		if err := waitRetry(ctx, i); err != nil {
			return 0, err
		}
	}
}

type RecvView struct {
	data []byte
}

func (v RecvView) Bytes() []byte {
	return v.data
}

func (v RecvView) Len() int {
	return len(v.data)
}

func (s *BoundSocket) TryRecvView() (RecvView, error) {
	ring, err := s.ensureRecvRing()
	if err != nil {
		return RecvView{}, err
	}
	return ring.tryRecvView()
}

func (s *BoundSocket) trySendRing(msg Message) (bool, error) {
	if s.socketType != Push && s.socketType != Scatter {
		return false, nil
	}
	parts := msg.partsView()
	if len(parts) != 1 {
		return false, nil
	}
	ring, err := s.ensureSendRing()
	if err != nil {
		return true, err
	}
	return ring.trySend(parts[0])
}

func (s *BoundSocket) ensureSendRing() (*sendRing, error) {
	if s.sendRing != nil {
		return s.sendRing, nil
	}
	ring, err := newSendRing(s.handle, s.ringSize)
	if err != nil {
		return nil, err
	}
	s.sendRing = ring
	return ring, nil
}

func (s *BoundSocket) ensureRecvRing() (*recvRing, error) {
	if s.recvRing != nil {
		return s.recvRing, nil
	}
	ring, err := newRecvRing(s.handle, s.ringSize)
	if err != nil {
		return nil, err
	}
	s.recvRing = ring
	return ring, nil
}

func (s *BoundSocket) close() error {
	err := socketRecvViewClearNative(s.handle)
	if s.recvRing != nil {
		s.recvRing.close()
		s.recvRing = nil
	}
	if s.sendRing == nil {
		return err
	}
	closeErr := s.sendRing.close()
	s.sendRing = nil
	if err != nil {
		return err
	}
	return closeErr
}
