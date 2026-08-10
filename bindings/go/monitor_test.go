package omq

import (
	"errors"
	"testing"
	"time"
)

func TestMonitorHandshakeEvent(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push, Identity([]byte("push-id")))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)
	monitor, err := pull.Monitor()
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()

	endpoint, err := pull.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	event := receiveMonitorKind(t, monitor, "HANDSHAKE_SUCCEEDED")
	if event.Endpoint != endpoint {
		t.Fatalf("event endpoint = %q, want %q", event.Endpoint, endpoint)
	}
	if !event.HasPeer {
		t.Fatal("monitor event has no peer info")
	}
	if event.Peer.SocketType != "PUSH" {
		t.Fatalf("peer socket type = %q, want PUSH", event.Peer.SocketType)
	}
	if string(event.Peer.Identity) != "push-id" {
		t.Fatalf("peer identity = %q, want push-id", event.Peer.Identity)
	}
	if event.Peer.ZMTPMajor != 3 || event.Peer.ZMTPMinor == 0 {
		t.Fatalf("ZMTP version = %d.%d, want 3.x", event.Peer.ZMTPMajor, event.Peer.ZMTPMinor)
	}
}

func TestMonitorTimeoutAndClose(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	monitor, err := pull.Monitor()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monitor.TryRecv(); !errors.Is(err, ErrAgain) {
		t.Fatalf("TryRecv err = %v, want ErrAgain", err)
	}
	if _, err := monitor.RecvTimeout(time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}
	monitor.Close()
	if _, err := monitor.TryRecv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryRecv after close err = %v, want ErrClosed", err)
	}
}

func receiveMonitorKind(t *testing.T, monitor *Monitor, kind string) MonitorEvent {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		event, err := monitor.RecvTimeout(200 * time.Millisecond)
		if errors.Is(err, ErrTimeout) || errors.Is(err, ErrAgain) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if event.Kind == kind {
			return event
		}
	}
	t.Fatalf("timed out waiting for monitor event %s", kind)
	return MonitorEvent{}
}
