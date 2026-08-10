package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunBlockingReceiveHonorsContext(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	if _, err := pull.Bind("inproc://go-run-cancel"); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := pull.Run(runCtx, func(socket *BoundSocket) error {
		buf := make([]byte, 16)
		_, err := socket.RecvIntoBlocking(buf)
		return err
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("Run err = %v, want ErrTimeout", err)
	}
}

func TestBoundRecvIntoUsesContextAndRing(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull.Bind("inproc://go-bound-recv-context")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("ring-context"), time.Second); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = pull.Run(runCtx, func(socket *BoundSocket) error {
		buf := make([]byte, 32)
		n, err := socket.RecvInto(runCtx, buf)
		if err != nil {
			return err
		}
		if got := string(buf[:n]); got != "ring-context" {
			t.Fatalf("message = %q, want ring-context", got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCloseContextReturnsWhileRunCallbackIsActive(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)

	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		_ = pull.Run(context.Background(), func(socket *BoundSocket) error {
			close(started)
			<-release
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Run did not start")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := pull.Close(closeCtx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Close err = %v, want ErrTimeout", err)
	}
	close(release)

	done := make(chan error, 1)
	go func() {
		done <- pull.Close(context.Background())
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("socket close did not finish")
	}
}
