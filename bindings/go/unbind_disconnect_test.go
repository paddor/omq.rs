package omq

import (
	"testing"
	"time"
)

func TestDisconnectThenReconnect(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
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
	if err := push.Disconnect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("again"), time.Second); err != nil {
		t.Fatal(err)
	}
	assertRecvString(t, pull, "again")
}

func TestUnbindThenBindAgain(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := pull.Unbind(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := pull.Bind(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := push.WaitConnectedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("after-rebind"), time.Second); err != nil {
		t.Fatal(err)
	}
	assertRecvString(t, pull, "after-rebind")
}
