package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOpenRejectsInvalidConfig(t *testing.T) {
	if _, err := Open(Config{IOThreads: -1}); err == nil {
		t.Fatal("Open with negative IOThreads succeeded")
	}
	if _, err := Open(Config{RingSize: -1}); err == nil {
		t.Fatal("Open with negative RingSize succeeded")
	}
}

func TestContextCloseClosesOwnedSockets(t *testing.T) {
	ctx := openTestContext(t)
	pull := newTestSocket(t, ctx, Pull)

	closeContext(t, ctx)

	if _, err := pull.TryRecv(); !errors.Is(err, ErrClosed) {
		t.Fatalf("TryRecv err = %v, want ErrClosed", err)
	}
	if _, err := ctx.Socket(Push); !errors.Is(err, ErrClosed) {
		t.Fatalf("Socket after close err = %v, want ErrClosed", err)
	}
}

func TestSharedContextCloseDoesNotTerminateOwner(t *testing.T) {
	owner := openTestContext(t)
	defer closeContext(t, owner)

	key, err := owner.ShareKey()
	if err != nil {
		t.Fatal(err)
	}
	shared, err := OpenShared(key)
	if err != nil {
		t.Fatal(err)
	}
	closeContext(t, shared)

	socket := newTestSocket(t, owner, Pull)
	defer closeSocket(t, socket)
}

func TestContextCloseContextCanTimeOutWhileCloseContinues(t *testing.T) {
	ctx := openTestContext(t)
	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)

	runStarted := make(chan struct{})
	releaseRun := make(chan struct{})
	go func() {
		_ = pull.Run(context.Background(), func(socket *BoundSocket) error {
			close(runStarted)
			<-releaseRun
			return nil
		})
	}()
	select {
	case <-runStarted:
	case <-time.After(time.Second):
		t.Fatal("Run did not start")
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := ctx.CloseContext(closeCtx); !errors.Is(err, ErrTimeout) {
		t.Fatalf("CloseContext err = %v, want ErrTimeout", err)
	}
	close(releaseRun)

	done := make(chan error, 1)
	go func() {
		done <- ctx.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("context close did not finish")
	}
}
