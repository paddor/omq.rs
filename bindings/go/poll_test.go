package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPollerReceivesFromMultipleSockets(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull1 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull1)
	pull2 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull2)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull2.Bind("inproc://go-poller")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	poller, err := NewPoller(pull1, pull2)
	if err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("poller"), time.Second); err != nil {
		t.Fatal(err)
	}

	event, err := poller.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if event.Socket != pull2 || event.Message.String() != "poller" {
		t.Fatalf("event = %#v", event)
	}
}

func TestReceiveAnyDoesNotConsumeLosingSocket(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull1 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull1)
	pull2 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull2)
	push1 := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push1)
	push2 := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push2)

	endpoint1, err := pull1.Bind("inproc://go-receive-any-loser-1")
	if err != nil {
		t.Fatal(err)
	}
	endpoint2, err := pull2.Bind("inproc://go-receive-any-loser-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := push1.Connect(endpoint1); err != nil {
		t.Fatal(err)
	}
	if err := push2.Connect(endpoint2); err != nil {
		t.Fatal(err)
	}
	if err := push1.SendTimeout(String("first"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push2.SendTimeout(String("second"), time.Second); err != nil {
		t.Fatal(err)
	}

	event, err := ReceiveAnyTimeout(time.Second, pull1, pull2)
	if err != nil {
		t.Fatal(err)
	}
	if event.Socket != pull1 || event.Message.String() != "first" {
		t.Fatalf("first event = %#v", event)
	}
	msg, err := pull2.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "second" {
		t.Fatalf("losing socket message = %q, want second", got)
	}
}

func TestPollerRotatesReadySockets(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull1 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull1)
	pull2 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull2)
	push1 := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push1)
	push2 := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push2)

	endpoint1, err := pull1.Bind("inproc://go-poller-rotate-1")
	if err != nil {
		t.Fatal(err)
	}
	endpoint2, err := pull2.Bind("inproc://go-poller-rotate-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := push1.Connect(endpoint1); err != nil {
		t.Fatal(err)
	}
	if err := push2.Connect(endpoint2); err != nil {
		t.Fatal(err)
	}
	if err := push1.SendTimeout(String("one"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push1.SendTimeout(String("one-again"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push2.SendTimeout(String("two"), time.Second); err != nil {
		t.Fatal(err)
	}

	poller, err := NewPoller(pull1, pull2)
	if err != nil {
		t.Fatal(err)
	}
	first, err := poller.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	second, err := poller.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Socket != pull1 || first.Message.String() != "one" {
		t.Fatalf("first event = %#v", first)
	}
	if second.Socket != pull2 || second.Message.String() != "two" {
		t.Fatalf("second event = %#v", second)
	}
}

func TestReceiveAnyContextCancellation(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	if _, err := pull.Bind("inproc://go-receive-any-cancel"); err != nil {
		t.Fatal(err)
	}

	recvCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := ReceiveAny(recvCtx, pull); !errors.Is(err, ErrTimeout) {
		t.Fatalf("ReceiveAny err = %v, want ErrTimeout", err)
	}
}

func TestReceiveAnyConcurrentClose(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	if _, err := pull.Bind("inproc://go-receive-any-close"); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := ReceiveAny(context.Background(), pull)
		errCh <- err
	}()
	time.Sleep(time.Millisecond)
	if err := pull.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("ReceiveAny err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReceiveAny did not unblock after close")
	}
}

func TestPollerRejectsInvalidSockets(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)

	if _, err := NewPoller(); !isConfigError(err) {
		t.Fatalf("NewPoller empty err = %v, want ConfigError", err)
	}
	if _, err := NewPoller(pull, pull); !isConfigError(err) {
		t.Fatalf("NewPoller duplicate err = %v, want ConfigError", err)
	}
}
