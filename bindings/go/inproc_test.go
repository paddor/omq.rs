package omq

import (
	"errors"
	"testing"
	"time"
)

func TestInprocNamesAreContextLocal(t *testing.T) {
	endpoint := "inproc://go-isolated"
	a := openTestContext(t)
	defer closeContext(t, a)
	b := openTestContext(t)
	defer closeContext(t, b)

	pull := newTestSocket(t, a, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, b, Push)
	defer closeSocket(t, push)

	if _, err := pull.Bind(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	_ = push.SendTimeout(String("hidden"), 50*time.Millisecond)
	if _, err := pull.RecvTimeout(100 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}
}

func TestInprocWorksAcrossSharedContexts(t *testing.T) {
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
	defer closeContext(t, shared)

	pull := newTestSocket(t, owner, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, shared, Push)
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-shared-contexts")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("visible"), time.Second); err != nil {
		t.Fatal(err)
	}
	assertRecvString(t, pull, "visible")
}
