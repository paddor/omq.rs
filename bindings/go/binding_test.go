package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInprocPushPull(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint := "inproc://go-push-pull"
	bound, err := pull.Bind(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}

	sendCtx, cancelSend := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelSend()
	if err := push.Send(sendCtx, String("hello")); err != nil {
		t.Fatal(err)
	}

	msg, err := pull.RecvTimeout(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "hello" {
		t.Fatalf("message = %q, want hello", got)
	}
}

func TestMultipart(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull.Bind("inproc://go-multipart")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}

	want := Multipart([]byte("a"), []byte("b"), []byte("c"))
	if err := push.SendTimeout(want, 2*time.Second); err != nil {
		t.Fatal(err)
	}

	got, err := pull.RecvTimeout(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	parts := got.Parts()
	if len(parts) != 3 || string(parts[0]) != "a" || string(parts[1]) != "b" || string(parts[2]) != "c" {
		t.Fatalf("parts = %#v", parts)
	}
}

func TestTimeoutsAndTry(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	if _, err := pull.Bind("inproc://go-timeouts"); err != nil {
		t.Fatal(err)
	}

	if _, err := pull.TryRecv(); !errors.Is(err, ErrAgain) {
		t.Fatalf("TryRecv err = %v, want ErrAgain", err)
	}
	if _, err := pull.RecvTimeout(5 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}
}

func TestChannels(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull.Bind("inproc://go-channels")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pullCh, err := pull.Channels(runCtx, ChannelOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer pullCh.Close()
	pushCh, err := push.Channels(runCtx, ChannelOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer pushCh.Close()

	if pullCh.Tx != nil {
		t.Fatal("PULL Tx is not nil")
	}
	if pushCh.Rx != nil {
		t.Fatal("PUSH Rx is not nil")
	}

	select {
	case pushCh.Tx <- String("via-channel"):
	case <-runCtx.Done():
		t.Fatal(runCtx.Err())
	}

	select {
	case msg := <-pullCh.Rx:
		if got := msg.String(); got != "via-channel" {
			t.Fatalf("message = %q, want via-channel", got)
		}
	case err := <-pullCh.Errors:
		t.Fatal(err)
	case <-runCtx.Done():
		t.Fatal(runCtx.Err())
	}
}

func TestMonitorListening(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	monitor, err := pull.Monitor()
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()

	if _, err := pull.Bind("inproc://go-monitor"); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		event, err := monitor.Recv(runCtx)
		if err != nil {
			t.Fatal(err)
		}
		if event.Kind == "LISTENING" {
			return
		}
	}
}

func TestSocketTypeCreation(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	types := []SocketType{
		Pair, Pub, Sub, Req, Rep, Dealer, Router, Pull, Push, XPub,
		XSub, Stream, Server, Client, Radio, Dish, Gather, Scatter, Peer, Channel,
	}
	for _, socketType := range types {
		socket := newTestSocket(t, ctx, socketType)
		closeSocket(t, socket)
	}
}

func TestLZ4TCP(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull.Bind("lz4+tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("compressed"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "compressed" {
		t.Fatalf("message = %q, want compressed", got)
	}
}

func openTestContext(t *testing.T) *Context {
	t.Helper()
	ctx, err := Open(Config{IOThreads: 2})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func newTestSocket(t *testing.T, ctx *Context, socketType SocketType) *Socket {
	t.Helper()
	socket, err := ctx.Socket(socketType)
	if err != nil {
		t.Fatal(err)
	}
	return socket
}

func closeSocket(t *testing.T, socket *Socket) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := socket.Close(ctx); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func closeContext(t *testing.T, ctx *Context) {
	t.Helper()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ctx.CloseContext(closeCtx); err != nil && !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
