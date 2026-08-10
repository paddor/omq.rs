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
	handle    *nativeContext
	ringSize  int
	overrun   OverrunPolicy
	mu        sync.Mutex
	sockets   map[*Socket]struct{}
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
	ctx := &Context{
		handle:    handle,
		ringSize:  config.RingSize,
		overrun:   config.OverrunPolicy,
		sockets:   make(map[*Socket]struct{}),
		closeDone: make(chan struct{}),
	}
	runtime.SetFinalizer(ctx, (*Context).free)
	return ctx, nil
}

// OpenShared imports a process-local shared native context.
func OpenShared(key ShareKey) (*Context, error) {
	handle, err := contextFromShareKeyNative(key)
	if err != nil {
		return nil, err
	}
	ctx := &Context{
		handle:    handle,
		sockets:   make(map[*Socket]struct{}),
		closeDone: make(chan struct{}),
	}
	runtime.SetFinalizer(ctx, (*Context).free)
	return ctx, nil
}

// ShareKey returns a process-local key for sharing this context.
func (c *Context) ShareKey() (ShareKey, error) {
	if c == nil || c.handle == nil || c.closed.Load() {
		return ShareKey{}, ErrClosed
	}
	key, err := contextShareKeyNative(c.handle)
	keepAlive(c)
	return key, err
}

// Socket creates a socket and applies pre-I/O options.
func (c *Context) Socket(socketType SocketType, opts ...SocketOption) (*Socket, error) {
	if c == nil || c.handle == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	handle, err := socketNewNative(c.handle, socketType)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	socket := newSocket(handle, socketType, c, c.ringSize, c.overrun)
	runtime.SetFinalizer(socket, (*Socket).free)
	c.sockets[socket] = struct{}{}
	c.mu.Unlock()
	for _, opt := range opts {
		if err := opt(socket); err != nil {
			socket.free()
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
	if c == nil || c.handle == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		go c.closeAll()
	})
	select {
	case <-c.closeDone:
		keepAlive(c)
		return nil
	case <-ctx.Done():
		return errFromContext(ctx)
	}
}

func (c *Context) free() {
	if c == nil || c.handle == nil {
		return
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeAll()
	})
	<-c.closeDone
}

func (c *Context) removeSocket(socket *Socket) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sockets, socket)
}

func (c *Context) closeAll() {
	defer close(c.closeDone)
	sockets := c.detachSockets()
	for _, socket := range sockets {
		_ = socket.Close(context.Background())
	}
	c.freeOnce.Do(func() {
		contextFreeNative(c.handle)
	})
	runtime.SetFinalizer(c, nil)
}

func (c *Context) detachSockets() []*Socket {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sockets) == 0 {
		return nil
	}
	sockets := make([]*Socket, 0, len(c.sockets))
	for socket := range c.sockets {
		sockets = append(sockets, socket)
	}
	c.sockets = make(map[*Socket]struct{})
	return sockets
}
