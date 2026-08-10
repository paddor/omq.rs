package omq

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSendCopiesBeforeCallerCanMutate(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-send-copy")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}

	body := []byte("before")
	if err := push.SendTimeout(Bytes(body), time.Second); err != nil {
		t.Fatal(err)
	}
	body[0] = 'x'
	assertRecvString(t, pull, "before")
}

func TestRecvIntoRejectsTooSmallDestination(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-recv-into-small")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("larger"), time.Second); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if _, err := pull.RecvIntoTimeout(buf, time.Second); !isMessageTooLarge(err) {
		t.Fatalf("RecvIntoTimeout err = %v, want MessageTooLargeError", err)
	}
}

func TestRecvIntoRejectsMultipart(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-recv-into-multipart")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(Multipart([]byte("a"), []byte("b")), time.Second); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 8)
	if _, err := pull.RecvIntoTimeout(buf, time.Second); !isConfigError(err) {
		t.Fatalf("RecvIntoTimeout err = %v, want ConfigError", err)
	}
}

func TestReqRepRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	rep := newTestSocket(t, ctx, Rep)
	defer closeSocket(t, rep)
	req := newTestSocket(t, ctx, Req)
	defer closeSocket(t, req)

	endpoint, err := rep.Bind("inproc://go-req-rep")
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := req.SendTimeout(String("question"), time.Second); err != nil {
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

func TestCloseIsIdempotent(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	if err := pull.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pull.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func isMessageTooLarge(err error) bool {
	var tooLarge *MessageTooLargeError
	return errors.As(err, &tooLarge)
}
