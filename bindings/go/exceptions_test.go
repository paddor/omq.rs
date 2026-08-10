package omq

import (
	"errors"
	"testing"
	"time"
)

func TestEndpointErrorsAreTyped(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)

	if _, err := pull.Bind("not-an-endpoint"); !isEndpointError(err) {
		t.Fatalf("Bind err = %v, want EndpointError", err)
	}
	if err := pull.Connect("nosuch://host"); !isEndpointError(err) {
		t.Fatalf("Connect err = %v, want EndpointError", err)
	}
}

func TestClosedSocketReturnsTypedError(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	closeSocket(t, pull)
	if _, err := pull.RecvTimeout(time.Millisecond); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecvTimeout err = %v, want ErrClosed", err)
	}
}

func TestMaxMessageSizeDropsOversizedReceive(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull, err := ctx.Socket(Pull, MaxMessageSize(8))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

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
	if err := push.SendTimeout(String("too-large-message"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := pull.RecvTimeout(200 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}
}

func isEndpointError(err error) bool {
	var endpoint *EndpointError
	return errors.As(err, &endpoint)
}
