package omq

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Socket is a goroutine-safe OMQ socket handle.
type Socket struct {
	handle     *nativeSocket
	socketType SocketType
	owner      *Context
	ringSize   int
	overrun    OverrunPolicy
	handleMu   sync.RWMutex
	closed     atomic.Bool
	ops        chan socketOp
	ownerDone  chan struct{}
	closeDone  chan struct{}
	closeOnce  sync.Once
	closeErr   error
	authMu     sync.Mutex
	authIDs    []uint64
	optionsMu  sync.RWMutex
	options    SocketOptions
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
		closeDone:  make(chan struct{}),
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
		cancel := cancelNewNative()
		if cancel == nil {
			return socketResult{err: &Error{Err: "run cancellation allocation failed"}}
		}
		cancelRegisterCurrentNative(cancel)
		cancelDone := make(chan struct{})
		stopCancel := context.AfterFunc(ctx, func() {
			cancelNative(cancel)
			close(cancelDone)
		})
		bound := &BoundSocket{
			handle:     handle,
			socketType: op.socketType,
			ringSize:   op.ringSize,
			ctx:        ctx,
			cancel:     cancel,
		}
		err := op.run(bound)
		if !stopCancel() {
			<-cancelDone
		}
		closeErr := bound.close()
		cancelFreeNative(cancel)
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

// Bind binds this socket to an endpoint and returns the bound endpoint.
func (s *Socket) Bind(endpoint string) (string, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpBind, endpoint: endpoint})
	if err != nil {
		return "", err
	}
	keepAlive(s)
	return result.text, nil
}

// Connect connects this socket to an endpoint.
func (s *Socket) Connect(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpConnect, endpoint: endpoint})
	keepAlive(s)
	return err
}

// Unbind removes a previously bound endpoint.
func (s *Socket) Unbind(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpUnbind, endpoint: endpoint})
	keepAlive(s)
	return err
}

// Disconnect removes a previously connected endpoint.
func (s *Socket) Disconnect(endpoint string) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpDisconnect, endpoint: endpoint})
	keepAlive(s)
	return err
}

// Send sends a message, waiting until ctx is done or the socket accepts it.
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

// TrySend sends a message without waiting for queue capacity.
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

// SendTimeout sends a message with libzmq-style timeout semantics.
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

// Recv receives the next complete message.
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

// RecvInto receives a single-part message into dst.
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

// TryRecv receives one message without waiting.
func (s *Socket) TryRecv() (Message, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpRecv})
	if err != nil {
		return Message{}, err
	}
	keepAlive(s)
	return result.message, nil
}

// TryRecvInto receives one single-part message into dst without waiting.
func (s *Socket) TryRecvInto(dst []byte) (int, error) {
	result, err := s.do(context.Background(), false, socketOp{kind: socketOpRecvInto, buffer: dst})
	if err != nil {
		return 0, err
	}
	keepAlive(s)
	return result.count, nil
}

// RecvTimeout receives one message with libzmq-style timeout semantics.
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

// RecvIntoTimeout receives one single-part message into dst with timeout semantics.
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

// Subscribe adds a SUB prefix.
func (s *Socket) Subscribe(prefix []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpSubscribe, data: prefix})
	keepAlive(s)
	return err
}

// SubscribeString adds a SUB prefix from a string.
func (s *Socket) SubscribeString(prefix string) error {
	return s.Subscribe([]byte(prefix))
}

// Unsubscribe removes a SUB prefix.
func (s *Socket) Unsubscribe(prefix []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpUnsubscribe, data: prefix})
	keepAlive(s)
	return err
}

// UnsubscribeString removes a SUB prefix from a string.
func (s *Socket) UnsubscribeString(prefix string) error {
	return s.Unsubscribe([]byte(prefix))
}

// Join adds a DISH group.
func (s *Socket) Join(group []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpJoin, data: group})
	keepAlive(s)
	return err
}

// JoinString adds a DISH group from a string.
func (s *Socket) JoinString(group string) error {
	return s.Join([]byte(group))
}

// Leave removes a DISH group.
func (s *Socket) Leave(group []byte) error {
	_, err := s.do(context.Background(), false, socketOp{kind: socketOpLeave, data: group})
	keepAlive(s)
	return err
}

// LeaveString removes a DISH group from a string.
func (s *Socket) LeaveString(group string) error {
	return s.Leave([]byte(group))
}

// WaitConnected waits until at least minPeers are connected.
func (s *Socket) WaitConnected(ctx context.Context, minPeers int) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var last int
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return last, err
		}
		count, err := s.waitConnectedOnce(ctx, minPeers)
		if err == nil {
			return count, nil
		}
		if !errors.Is(err, ErrTimeout) {
			return count, err
		}
		last = count
		if err := waitRetry(ctx, i); err != nil {
			return last, err
		}
	}
}

// WaitConnectedTimeout waits until at least minPeers are connected.
func (s *Socket) WaitConnectedTimeout(minPeers int, timeout time.Duration) (int, error) {
	if timeout == 0 {
		return s.waitConnectedOnce(context.Background(), minPeers)
	}
	if timeout < 0 {
		return s.WaitConnected(context.Background(), minPeers)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.WaitConnected(ctx, minPeers)
}

func (s *Socket) waitConnectedOnce(ctx context.Context, minPeers int) (int, error) {
	value, err := s.call(ctx, false, func(handle *nativeSocket) (any, error) {
		return socketWaitConnectedNative(handle, minPeers, 0)
	})
	if err != nil {
		return 0, err
	}
	return value.(int), nil
}

// WaitSubscribed waits until at least minSubscriptions are visible.
func (s *Socket) WaitSubscribed(ctx context.Context, minSubscriptions uint64) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var last uint64
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return last, err
		}
		count, err := s.waitSubscribedOnce(ctx, minSubscriptions)
		if err == nil {
			return count, nil
		}
		if !errors.Is(err, ErrTimeout) {
			return count, err
		}
		last = count
		if err := waitRetry(ctx, i); err != nil {
			return last, err
		}
	}
}

// WaitSubscribedTimeout waits until at least minSubscriptions are visible.
func (s *Socket) WaitSubscribedTimeout(minSubscriptions uint64, timeout time.Duration) (uint64, error) {
	if timeout == 0 {
		return s.waitSubscribedOnce(context.Background(), minSubscriptions)
	}
	if timeout < 0 {
		return s.WaitSubscribed(context.Background(), minSubscriptions)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.WaitSubscribed(ctx, minSubscriptions)
}

func (s *Socket) waitSubscribedOnce(ctx context.Context, minSubscriptions uint64) (uint64, error) {
	value, err := s.call(ctx, false, func(handle *nativeSocket) (any, error) {
		return socketWaitSubscribedNative(handle, minSubscriptions, 0)
	})
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

// Close closes the socket using configured linger.
func (s *Socket) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.startClose(socketOp{kind: socketOpClose, useConfigured: true})
	})
	keepAlive(s)
	return s.waitClose(ctx)
}

// CloseLinger closes the socket with an explicit linger.
func (s *Socket) CloseLinger(ctx context.Context, linger time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.ops == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.startClose(socketOp{kind: socketOpClose, linger: linger})
	})
	keepAlive(s)
	return s.waitClose(ctx)
}

// Type returns this socket's type.
func (s *Socket) Type() SocketType {
	if s == nil {
		return 0
	}
	return s.socketType
}

// Options returns a copy of options configured through OMQ.go.
func (s *Socket) Options() SocketOptions {
	if s == nil {
		return SocketOptions{}
	}
	s.optionsMu.RLock()
	defer s.optionsMu.RUnlock()
	return cloneSocketOptions(s.options)
}

// Run executes fn on the socket owner goroutine.
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

func (s *Socket) startClose(op socketOp) {
	s.closed.Store(true)
	go func() {
		_, err := s.do(context.Background(), true, op)
		close(s.ops)
		<-s.ownerDone
		s.handleMu.Lock()
		s.handle = nil
		s.handleMu.Unlock()
		s.releaseAuthCallbacks()
		if s.owner != nil {
			s.owner.removeSocket(s)
		}
		s.closeErr = err
		close(s.closeDone)
	}()
}

func (s *Socket) waitClose(ctx context.Context) error {
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return errFromContext(ctx)
	}
}

func (s *Socket) noFinalizer() {
	runtime.SetFinalizer(s, nil)
}

func (s *Socket) addAuthCallback(id uint64) {
	s.authMu.Lock()
	s.authIDs = append(s.authIDs, id)
	s.authMu.Unlock()
}

func (s *Socket) releaseAuthCallbacks() {
	s.authMu.Lock()
	ids := s.authIDs
	s.authIDs = nil
	s.authMu.Unlock()
	for _, id := range ids {
		unregisterAuthCallback(id)
	}
}

func (s *Socket) recordOption(record func(*SocketOptions)) {
	if record == nil || s == nil {
		return
	}
	s.optionsMu.Lock()
	record(&s.options)
	s.optionsMu.Unlock()
}

func (s *Socket) nativeHandle() (*nativeSocket, error) {
	if s == nil || s.closed.Load() {
		return nil, ErrClosed
	}
	s.handleMu.RLock()
	defer s.handleMu.RUnlock()
	if s.handle == nil {
		return nil, ErrClosed
	}
	return s.handle, nil
}

func cloneSocketOptions(options SocketOptions) SocketOptions {
	options.Identity.Value = append([]byte(nil), options.Identity.Value...)
	options.CompressionDict.Value = append([]byte(nil), options.CompressionDict.Value...)
	return options
}

// BoundSocket is a socket handle valid only inside Socket.Run.
type BoundSocket struct {
	handle     *nativeSocket
	socketType SocketType
	ringSize   int
	ctx        context.Context
	cancel     *nativeCancel
	sendRing   *sendRing
	recvRing   *recvRing
}

// Context returns the Run context.
func (s *BoundSocket) Context() context.Context {
	if s == nil || s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

// Send sends a message from the owner goroutine.
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

// TrySend sends without waiting from the owner goroutine.
func (s *BoundSocket) TrySend(msg Message) error {
	handled, err := s.trySendRing(msg)
	if handled {
		return err
	}
	return socketMessageSendNative(s.handle, msg)
}

// SendBlocking sends using the Run context until accepted or canceled.
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

// RecvInto receives a single-part message into dst.
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

// TryRecvInto receives a single-part message into dst without waiting.
func (s *BoundSocket) TryRecvInto(dst []byte) (int, error) {
	ring, err := s.ensureRecvRing()
	if err != nil {
		return 0, err
	}
	return ring.tryRecvInto(dst)
}

// RecvIntoBlocking receives into dst using the Run context.
func (s *BoundSocket) RecvIntoBlocking(dst []byte) (int, error) {
	ctx := s.Context()
	ring, err := s.ensureRecvRing()
	if err != nil {
		return 0, err
	}
	for {
		if err := errFromContext(ctx); err != nil {
			return 0, err
		}
		n, err := ring.recvIntoCancelable(dst, s.cancel)
		if err == nil {
			return n, nil
		}
		if errors.Is(err, ErrCanceled) {
			if ctxErr := errFromContext(ctx); ctxErr != nil {
				return 0, ctxErr
			}
		}
		if !errors.Is(err, ErrAgain) {
			return 0, err
		}
	}
}

// RecvView receives a borrowed single-part payload and passes it to fn.
//
// The payload slice is valid only until fn returns. fn must not retain it.
func (s *BoundSocket) RecvView(ctx context.Context, fn func([]byte) error) error {
	if fn == nil {
		return &ConfigError{Err: "nil RecvView callback"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return err
		}
		delivered, err := s.tryRecvView(fn)
		if delivered {
			return err
		}
		if !errors.Is(err, ErrAgain) {
			return err
		}
		if err := waitRetry(ctx, i); err != nil {
			return err
		}
	}
}

// TryRecvView receives a borrowed single-part payload without waiting.
//
// The payload slice is valid only until fn returns. fn must not retain it.
func (s *BoundSocket) TryRecvView(fn func([]byte) error) error {
	if fn == nil {
		return &ConfigError{Err: "nil RecvView callback"}
	}
	_, err := s.tryRecvView(fn)
	return err
}

func (s *BoundSocket) tryRecvView(fn func([]byte) error) (bool, error) {
	ring, err := s.ensureRecvRing()
	if err != nil {
		return false, err
	}
	return ring.tryRecvView(fn)
}

// RecvViewBlocking receives a borrowed payload using the Run context.
//
// The payload slice is valid only until fn returns. fn must not retain it.
func (s *BoundSocket) RecvViewBlocking(fn func([]byte) error) error {
	if fn == nil {
		return &ConfigError{Err: "nil RecvView callback"}
	}
	ctx := s.Context()
	ring, err := s.ensureRecvRing()
	if err != nil {
		return err
	}
	for {
		if err := errFromContext(ctx); err != nil {
			return err
		}
		delivered, err := ring.recvViewCancelable(s.cancel, fn)
		if delivered {
			return err
		}
		if errors.Is(err, ErrCanceled) {
			if ctxErr := errFromContext(ctx); ctxErr != nil {
				return ctxErr
			}
		}
		if !errors.Is(err, ErrAgain) {
			return err
		}
	}
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
	if s.recvRing != nil {
		s.recvRing.close()
		s.recvRing = nil
	}
	if s.sendRing == nil {
		return nil
	}
	closeErr := s.sendRing.close()
	s.sendRing = nil
	return closeErr
}
