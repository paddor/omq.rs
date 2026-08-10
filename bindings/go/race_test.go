package omq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

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
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := push.Send(sendCtx, String("msg"))
				cancel()
				if err != nil {
					t.Errorf("worker %d send %d: %v", worker, i, err)
					return
				}
			}
		}(worker)
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
