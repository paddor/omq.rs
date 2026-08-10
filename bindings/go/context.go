package omq

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Config controls native context and Go ring defaults.
type Config struct {
	// IOThreads sets native OMQ I/O thread count.
	IOThreads int
	// RingSize sets private Go/native ring descriptor capacity.
	RingSize int
	// OverrunPolicy sets default channel overrun behavior.
	OverrunPolicy OverrunPolicy
}

// Context owns a native OMQ context and its sockets.
type Context struct {
	state   *contextState
	cleanup runtime.Cleanup
}

type contextState struct {
	handle    *nativeContext
	ringSize  int
	overrun   OverrunPolicy
	mu        sync.Mutex
	sockets   map[*socketState]struct{}
	closed    atomic.Bool
	closeOnce sync.Once
	freeOnce  sync.Once
	closeDone chan struct{}
}

// Open creates a native OMQ context.
func Open(config Config) (*Context, error) {
	ioThreads := config.IOThreads
	if ioThreads == 0 {
		ioThreads = 1
	}
	if ioThreads < 0 {
		return nil, &ConfigError{Err: "io_threads must be greater than zero"}
	}
	if config.RingSize < 0 {
		return nil, &ConfigError{Err: "ring size must be non-negative"}
	}
	handle, err := contextOpenNative(ioThreads)
	if err != nil {
		return nil, err
	}
	state := &contextState{
		handle:    handle,
		ringSize:  config.RingSize,
		overrun:   config.OverrunPolicy,
		sockets:   make(map[*socketState]struct{}),
		closeDone: make(chan struct{}),
	}
	ctx := &Context{state: state}
	ctx.cleanup = runtime.AddCleanup(ctx, cleanupContextState, state)
	return ctx, nil
}

// OpenShared imports a process-local shared native context.
func OpenShared(key ShareKey) (*Context, error) {
	handle, err := contextFromShareKeyNative(key)
	if err != nil {
		return nil, err
	}
	state := &contextState{
		handle:    handle,
		sockets:   make(map[*socketState]struct{}),
		closeDone: make(chan struct{}),
	}
	ctx := &Context{state: state}
	ctx.cleanup = runtime.AddCleanup(ctx, cleanupContextState, state)
	return ctx, nil
}

// ShareKey returns a process-local key for sharing this context.
func (c *Context) ShareKey() (ShareKey, error) {
	state := c.stateOrNil()
	if state == nil || state.closed.Load() {
		return ShareKey{}, ErrClosed
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed.Load() || state.handle == nil {
		return ShareKey{}, ErrClosed
	}
	key, err := contextShareKeyNative(state.handle)
	keepAlive(c)
	return key, err
}

// Socket creates a socket and applies pre-I/O options.
func (c *Context) Socket(socketType SocketType, opts ...SocketOption) (*Socket, error) {
	state := c.stateOrNil()
	if state == nil || state.closed.Load() {
		return nil, ErrClosed
	}
	state.mu.Lock()
	if state.closed.Load() || state.handle == nil {
		state.mu.Unlock()
		return nil, ErrClosed
	}
	handle, err := socketNewNative(state.handle, socketType)
	if err != nil {
		state.mu.Unlock()
		return nil, err
	}
	socket := newSocket(handle, socketType, c, state, state.ringSize, state.overrun)
	state.sockets[socket.state] = struct{}{}
	state.mu.Unlock()
	for _, opt := range opts {
		if err := opt(socket); err != nil {
			_ = socket.Close(context.Background())
			return nil, err
		}
	}
	keepAlive(c)
	return socket, nil
}

// Close closes all owned sockets and terminates the context.
func (c *Context) Close() error {
	return c.CloseContext(context.Background())
}

// CloseContext closes the context, bounded by ctx.
func (c *Context) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	state := c.stateOrNil()
	if state == nil {
		return nil
	}
	state.startClose()
	select {
	case <-state.closeDone:
		c.cleanup.Stop()
		keepAlive(c)
		return nil
	case <-ctx.Done():
		keepAlive(c)
		return errFromContext(ctx)
	}
}

func cleanupContextState(state *contextState) {
	if state == nil {
		return
	}
	go state.closeBackground()
}

func (c *Context) stateOrNil() *contextState {
	if c == nil {
		return nil
	}
	return c.state
}

func (state *contextState) startClose() {
	state.closeOnce.Do(func() {
		state.closed.Store(true)
		go state.closeAll()
	})
}

func (state *contextState) closeBackground() {
	state.closeOnce.Do(func() {
		state.closed.Store(true)
		state.closeAll()
	})
}

func (state *contextState) removeSocket(socket *socketState) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.sockets, socket)
}

func (state *contextState) closeAll() {
	defer close(state.closeDone)
	sockets := state.detachSockets()
	for _, socket := range sockets {
		_ = socket.close(context.Background(), socketOp{kind: socketOpClose, useConfigured: true})
	}
	state.freeOnce.Do(func() {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.handle != nil {
			contextFreeNative(state.handle)
			state.handle = nil
		}
	})
}

func (state *contextState) detachSockets() []*socketState {
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.sockets) == 0 {
		return nil
	}
	sockets := make([]*socketState, 0, len(state.sockets))
	for socket := range state.sockets {
		sockets = append(sockets, socket)
	}
	state.sockets = make(map[*socketState]struct{})
	return sockets
}
