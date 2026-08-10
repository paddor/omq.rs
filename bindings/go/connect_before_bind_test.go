package omq

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestConnectBeforeBindPushPullTCP(t *testing.T) {
	for _, delay := range []time.Duration{0, 50 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx := openTestContext(t)
			defer closeContext(t, ctx)

			push := newTestSocket(t, ctx, Push)
			defer closeSocket(t, push)
			pull := newTestSocket(t, ctx, Pull)
			defer closeSocket(t, pull)

			endpoint := freeTCPEndpoint(t)
			if err := push.Connect(endpoint); err != nil {
				t.Fatal(err)
			}
			time.Sleep(delay)
			if _, err := pull.Bind(endpoint); err != nil {
				t.Fatal(err)
			}
			if err := push.SendTimeout(String("late"), time.Second); err != nil {
				t.Fatal(err)
			}
			msg, err := pull.RecvTimeout(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := msg.String(); got != "late" {
				t.Fatalf("message = %q, want late", got)
			}
		})
	}
}

func TestConnectBeforeBindReqRepTCP(t *testing.T) {
	for _, delay := range []time.Duration{0, 50 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx := openTestContext(t)
			defer closeContext(t, ctx)

			req := newTestSocket(t, ctx, Req)
			defer closeSocket(t, req)
			rep := newTestSocket(t, ctx, Rep)
			defer closeSocket(t, rep)

			endpoint := freeTCPEndpoint(t)
			if err := req.Connect(endpoint); err != nil {
				t.Fatal(err)
			}
			time.Sleep(delay)
			if _, err := rep.Bind(endpoint); err != nil {
				t.Fatal(err)
			}
			if err := req.SendTimeout(String("q"), time.Second); err != nil {
				t.Fatal(err)
			}
			query, err := rep.RecvTimeout(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := query.String(); got != "q" {
				t.Fatalf("query = %q, want q", got)
			}
			if err := rep.SendTimeout(String("a"), time.Second); err != nil {
				t.Fatal(err)
			}
			answer, err := req.RecvTimeout(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := answer.String(); got != "a" {
				t.Fatalf("answer = %q, want a", got)
			}
		})
	}
}

func TestConnectBeforeBindPairInproc(t *testing.T) {
	for _, delay := range []time.Duration{0, 50 * time.Millisecond, 250 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			ctx := openTestContext(t)
			defer closeContext(t, ctx)

			a := newTestSocket(t, ctx, Pair)
			defer closeSocket(t, a)
			b := newTestSocket(t, ctx, Pair)
			defer closeSocket(t, b)

			endpoint := "inproc://go-cbb-pair-" + delay.String()
			if err := a.Connect(endpoint); err != nil {
				t.Fatal(err)
			}
			time.Sleep(delay)
			if _, err := b.Bind(endpoint); err != nil {
				t.Fatal(err)
			}
			if err := a.SendTimeout(String("from-a"), time.Second); err != nil {
				t.Fatal(err)
			}
			msg, err := b.RecvTimeout(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := msg.String(); got != "from-a" {
				t.Fatalf("message = %q, want from-a", got)
			}
			if err := b.SendTimeout(String("from-b"), time.Second); err != nil {
				t.Fatal(err)
			}
			msg, err = a.RecvTimeout(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if got := msg.String(); got != "from-b" {
				t.Fatalf("message = %q, want from-b", got)
			}
		})
	}
}

func freeTCPEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("tcp://127.0.0.1:%d", port)
}
