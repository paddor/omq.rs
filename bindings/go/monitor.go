package omq

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Monitor receives native socket monitor events.
type Monitor struct {
	handle *nativeMonitor
	mu     sync.Mutex
	closed atomic.Bool
}

// MonitorEvent describes one native monitor event.
type MonitorEvent struct {
	// Kind is the native monitor event name.
	Kind string
	// Endpoint is the endpoint associated with the event.
	Endpoint string
	// PeerIdent is the native peer identifier when present.
	PeerIdent string
	// Peer is peer metadata when HasPeer is true.
	Peer PeerInfo
	// HasPeer reports whether Peer is populated.
	HasPeer bool
	// Reason is error or disconnect detail when present.
	Reason string
	// CommandName is the peer command name when present.
	CommandName string
	// Data is event payload bytes when present.
	Data []byte
	// ConnectionID is the native connection id when present.
	ConnectionID uint64
	// Retry is reconnect delay when present.
	Retry time.Duration
	// Attempt is reconnect attempt count when present.
	Attempt uint32
}

// Monitor opens a monitor stream for this socket.
func (s *Socket) Monitor() (*Monitor, error) {
	value, err := s.call(context.Background(), false, func(handle *nativeSocket) (any, error) {
		return monitorNewNative(handle)
	})
	if err != nil {
		return nil, err
	}
	monitor := &Monitor{handle: value.(*nativeMonitor)}
	runtime.SetFinalizer(monitor, (*Monitor).free)
	keepAlive(s)
	return monitor, nil
}

// Recv receives the next monitor event.
func (m *Monitor) Recv(ctx context.Context) (MonitorEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for i := 0; ; i++ {
		if err := errFromContext(ctx); err != nil {
			return MonitorEvent{}, err
		}
		event, err := m.TryRecv()
		if err == nil {
			return event, nil
		}
		if !errors.Is(err, ErrAgain) {
			return MonitorEvent{}, err
		}
		if err := waitRetry(ctx, i); err != nil {
			return MonitorEvent{}, err
		}
	}
}

// RecvTimeout receives a monitor event with timeout semantics.
func (m *Monitor) RecvTimeout(timeout time.Duration) (MonitorEvent, error) {
	if timeout == 0 {
		return m.TryRecv()
	}
	if timeout < 0 {
		return m.Recv(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return m.Recv(ctx)
}

// TryRecv receives a monitor event without waiting.
func (m *Monitor) TryRecv() (MonitorEvent, error) {
	if m == nil {
		return MonitorEvent{}, ErrClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.handle == nil || m.closed.Load() {
		return MonitorEvent{}, ErrClosed
	}
	event, err := monitorRecvNative(m.handle)
	keepAlive(m)
	return event, err
}

// Close closes the monitor stream.
func (m *Monitor) Close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Swap(true) {
		return
	}
	if m.handle != nil {
		monitorFreeNative(m.handle)
		m.handle = nil
	}
	runtime.SetFinalizer(m, nil)
	keepAlive(m)
}

func (m *Monitor) free() {
	m.Close()
}
