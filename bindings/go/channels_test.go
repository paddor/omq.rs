package omq

import (
	"context"
	"testing"
	"time"
)

func TestChannelsReportReceiveOverrun(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	bound, err := pull.Bind("inproc://go-channel-overrun")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(bound); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	channels, err := pull.Channels(runCtx, ChannelOptions{
		Capacity:      1,
		OverrunPolicy: OverrunReturnError,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer channels.Close()

	if err := push.SendTimeout(String("one"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := push.SendTimeout(String("two"), time.Second); err != nil {
		t.Fatal(err)
	}

	select {
	case err, ok := <-channels.Errors:
		if !ok {
			t.Fatal("Errors closed before overrun")
		}
		if err == nil {
			t.Fatal("overrun error is nil")
		}
	case <-runCtx.Done():
		t.Fatal(runCtx.Err())
	}
}

func TestChannelsCloseClosesErrors(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)

	runCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	channels, err := pull.Channels(runCtx, ChannelOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	channels.Close()

	select {
	case _, ok := <-channels.Errors:
		if ok {
			t.Fatal("Errors still open")
		}
	default:
		t.Fatal("Errors not closed")
	}
}
