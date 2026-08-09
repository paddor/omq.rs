package omq

import (
	"context"
	"runtime"
	"sync/atomic"
)

type Config struct {
	IOThreads     int
	RingSize      int
	OverrunPolicy OverrunPolicy
}

type Context struct {
	handle *nativeContext
	closed atomic.Bool
}

func Open(config Config) (*Context, error) {
	ioThreads := config.IOThreads
	if ioThreads == 0 {
		ioThreads = 1
	}
	handle, err := contextOpenNative(ioThreads)
	if err != nil {
		return nil, err
	}
	ctx := &Context{handle: handle}
	runtime.SetFinalizer(ctx, (*Context).free)
	return ctx, nil
}

func OpenShared(key ShareKey) (*Context, error) {
	handle, err := contextFromShareKeyNative(key)
	if err != nil {
		return nil, err
	}
	ctx := &Context{handle: handle}
	runtime.SetFinalizer(ctx, (*Context).free)
	return ctx, nil
}

func (c *Context) ShareKey() (ShareKey, error) {
	if c == nil || c.handle == nil || c.closed.Load() {
		return ShareKey{}, ErrClosed
	}
	key, err := contextShareKeyNative(c.handle)
	keepAlive(c)
	return key, err
}

func (c *Context) Socket(socketType SocketType, opts ...SocketOption) (*Socket, error) {
	if c == nil || c.handle == nil || c.closed.Load() {
		return nil, ErrClosed
	}
	handle, err := socketNewNative(c.handle, socketType)
	if err != nil {
		return nil, err
	}
	socket := newSocket(handle, socketType)
	runtime.SetFinalizer(socket, (*Socket).free)
	for _, opt := range opts {
		if err := opt(socket); err != nil {
			socket.free()
			return nil, err
		}
	}
	keepAlive(c)
	return socket, nil
}

func (c *Context) Close() error {
	return c.CloseContext(context.Background())
}

func (c *Context) CloseContext(ctx context.Context) error {
	if c == nil || c.handle == nil {
		return nil
	}
	if c.closed.Swap(true) {
		return nil
	}
	done := make(chan struct{})
	go func() {
		contextCloseNative(c.handle)
		close(done)
	}()
	select {
	case <-done:
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
	contextFreeNative(c.handle)
	c.handle = nil
}
