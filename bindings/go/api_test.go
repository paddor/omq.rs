package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCurveKeyHelpers(t *testing.T) {
	keypair, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	if len(keypair.Public) != 40 || len(keypair.Secret) != 40 {
		t.Fatalf("CURVE key lengths = %d/%d, want 40/40", len(keypair.Public), len(keypair.Secret))
	}
	public, err := CurvePublic(keypair.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if public != keypair.Public {
		t.Fatalf("derived public = %q, want %q", public, keypair.Public)
	}
}

func TestPlainTCP(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull, err := ctx.Socket(Pull, PlainServer("alice", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push, PlainClient("alice", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)

	bound, err := pull.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("plain"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "plain" {
		t.Fatalf("message = %q, want plain", got)
	}
}

func TestCurveTCP(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	serverKeypair, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	clientKeypair, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	pull, err := ctx.Socket(Pull, CurveServer(serverKeypair))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push, CurveClient(clientKeypair, serverKeypair.Public))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)

	bound, err := pull.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("curve"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "curve" {
		t.Fatalf("message = %q, want curve", got)
	}
}

func TestWaitSubscribed(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pub := newTestSocket(t, ctx, Pub)
	defer closeSocket(t, pub)
	sub := newTestSocket(t, ctx, Sub)
	defer closeSocket(t, sub)

	bound, err := pub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SubscribeString("topic/"); err != nil {
		t.Fatal(err)
	}
	if err := sub.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.WaitSubscribedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := pub.SendTimeout(String("topic/value"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "topic/value" {
		t.Fatalf("message = %q, want topic/value", got)
	}
}

func TestReceiveAny(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull1 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull1)
	pull2 := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull2)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull2.Bind("inproc://go-receive-any")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if _, err := TryReceiveAny(pull1, pull2); !errors.Is(err, ErrAgain) {
		t.Fatalf("TryReceiveAny err = %v, want ErrAgain", err)
	}
	if err := push.SendTimeout(String("either"), time.Second); err != nil {
		t.Fatal(err)
	}
	event, err := ReceiveAnyTimeout(time.Second, pull1, pull2)
	if err != nil {
		t.Fatal(err)
	}
	if event.Socket != pull2 {
		t.Fatal("ReceiveAny returned wrong socket")
	}
	if got := event.Message.String(); got != "either" {
		t.Fatalf("message = %q, want either", got)
	}
}

func TestProxyPushPull(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	frontend := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, frontend)
	backend := newTestSocket(t, ctx, Push)
	defer closeSocket(t, backend)
	source := newTestSocket(t, ctx, Push)
	defer closeSocket(t, source)
	sink := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, sink)

	frontendEndpoint, err := frontend.Bind("inproc://go-proxy-fe")
	if err != nil {
		t.Fatal(err)
	}
	backendEndpoint, err := sink.Bind("inproc://go-proxy-be")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Connect(frontendEndpoint); err != nil {
		t.Fatal(err)
	}
	if err := backend.Connect(backendEndpoint); err != nil {
		t.Fatal(err)
	}

	proxyCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- Proxy(proxyCtx, frontend, backend, ProxyOptions{})
	}()

	if err := source.SendTimeout(String("proxied"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := sink.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "proxied" {
		t.Fatalf("message = %q, want proxied", got)
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("Proxy err = %v, want ErrCanceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy did not stop")
	}
}

func TestMessageCopiesPublicBytes(t *testing.T) {
	input := []byte("abc")
	msg := Bytes(input)
	input[0] = 'x'
	if got := msg.String(); got != "abc" {
		t.Fatalf("message = %q, want abc", got)
	}
	out := msg.Bytes()
	out[1] = 'y'
	if got := msg.String(); got != "abc" {
		t.Fatalf("message after Bytes mutation = %q, want abc", got)
	}
	if part := msg.Part(0); string(part) != "abc" {
		t.Fatalf("part = %q, want abc", part)
	}
	if msg.ByteLen() != 3 || msg.IsMultipart() {
		t.Fatalf("message metadata wrong")
	}
}
