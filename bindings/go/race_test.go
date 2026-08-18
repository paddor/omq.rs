package omq

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestReqRepScalarCallsCanMoveBetweenThreads(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	rep := newTestSocket(t, ctx, Rep)
	defer closeSocket(t, rep)
	req := newTestSocket(t, ctx, Req)
	defer closeSocket(t, req)

	endpoint, err := rep.Bind("inproc://go-req-rep-thread-migration")
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Connect(endpoint); err != nil {
		t.Fatal(err)
	}

	sent := make(chan error, 1)
	releaseThread := make(chan struct{})
	defer close(releaseThread)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		sent <- req.SendTimeout(String("question"), time.Second)
		<-releaseThread
	}()
	if err := <-sent; err != nil {
		t.Fatal(err)
	}

	query, err := rep.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := query.String(); got != "question" {
		t.Fatalf("query = %q, want question", got)
	}
	if err := rep.SendTimeout(String("answer"), time.Second); err != nil {
		t.Fatal(err)
	}
	reply, err := req.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := reply.String(); got != "answer" {
		t.Fatalf("reply = %q, want answer", got)
	}
}

func TestReqScalarReceiveCancellationAndClose(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	rep := newTestSocket(t, ctx, Rep)
	defer closeSocket(t, rep)
	req := newTestSocket(t, ctx, Req)
	defer closeSocket(t, req)

	endpoint, err := rep.Bind("inproc://go-req-recv-cancel-close")
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := req.SendTimeout(String("question"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := req.RecvTimeout(5 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := req.Recv(context.Background())
		errCh <- err
	}()
	time.Sleep(time.Millisecond)
	if err := req.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Recv err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not unblock after close")
	}
}

func TestConcurrentSendReceive(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-concurrent-send-recv")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}

	const goroutines = 4
	const perGoroutine = 64
	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		wg.Go(func() {
			for i := 0; i < perGoroutine; i++ {
				sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := push.Send(sendCtx, String("msg"))
				cancel()
				if err != nil {
					t.Errorf("worker %d send %d: %v", worker, i, err)
					return
				}
			}
		})
	}

	for i := 0; i < goroutines*perGoroutine; i++ {
		msg, err := pull.RecvTimeout(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if got := msg.String(); got != "msg" {
			t.Fatalf("message = %q, want msg", got)
		}
	}
	wg.Wait()
}

func TestConcurrentCloseAndReceive(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	if _, err := pull.Bind("inproc://go-close-recv-race"); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := pull.Recv(context.Background())
		errCh <- err
	}()
	time.Sleep(time.Millisecond)
	if err := pull.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Recv err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not unblock after close")
	}
}
