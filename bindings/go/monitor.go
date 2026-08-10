package omq

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
)

type Monitor struct {
	handle *nativeMonitor
	closed atomic.Bool
}

type MonitorEvent struct {
	Kind         string
	Endpoint     string
	PeerIdent    string
	Peer         PeerInfo
	HasPeer      bool
	Reason       string
	CommandName  string
	Data         []byte
	ConnectionID uint64
	Retry        time.Duration
	Attempt      uint32
}

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

func (m *Monitor) TryRecv() (MonitorEvent, error) {
	if m == nil || m.handle == nil || m.closed.Load() {
		return MonitorEvent{}, ErrClosed
	}
	event, err := monitorRecvNative(m.handle)
	keepAlive(m)
	return event, err
}

func (m *Monitor) Close() {
	if m == nil || m.handle == nil {
		return
	}
	if m.closed.Swap(true) {
		return
	}
	monitorCloseNative(m.handle)
	keepAlive(m)
}

func (m *Monitor) free() {
	if m == nil || m.handle == nil {
		return
	}
	monitorFreeNative(m.handle)
	m.handle = nil
}
