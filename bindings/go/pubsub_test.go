package omq

import (
	"errors"
	"testing"
	"time"
)

func TestPubSubPrefixFilter(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pub := newTestSocket(t, ctx, Pub)
	defer closeSocket(t, pub)
	sub := newTestSocket(t, ctx, Sub)
	defer closeSocket(t, sub)

	endpoint, err := pub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SubscribeString("weather/"); err != nil {
		t.Fatal(err)
	}
	if err := sub.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.WaitSubscribedTimeout(1, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	if err := pub.SendTimeout(String("sports/score-12"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := pub.SendTimeout(String("weather/sunny"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := pub.SendTimeout(String("weather/rain"), time.Second); err != nil {
		t.Fatal(err)
	}
	assertRecvString(t, sub, "weather/sunny")
	assertRecvString(t, sub, "weather/rain")
	if _, err := sub.TryRecv(); !errors.Is(err, ErrAgain) {
		t.Fatalf("TryRecv err = %v, want ErrAgain", err)
	}
}

func TestPubSubUnsubscribeDropsTopic(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pub := newTestSocket(t, ctx, Pub)
	defer closeSocket(t, pub)
	sub := newTestSocket(t, ctx, Sub)
	defer closeSocket(t, sub)

	endpoint, err := pub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SubscribeString("a"); err != nil {
		t.Fatal(err)
	}
	if err := sub.SubscribeString("b"); err != nil {
		t.Fatal(err)
	}
	if err := sub.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := pub.WaitSubscribedTimeout(2, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := sub.UnsubscribeString("a"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if err := pub.SendTimeout(String("a-one"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := pub.SendTimeout(String("b-two"), time.Second); err != nil {
		t.Fatal(err)
	}
	assertRecvString(t, sub, "b-two")
	if _, err := sub.TryRecv(); !errors.Is(err, ErrAgain) {
		t.Fatalf("TryRecv err = %v, want ErrAgain", err)
	}
}

func TestXPubReceivesSubscriptionFrame(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	xpub := newTestSocket(t, ctx, XPub)
	defer closeSocket(t, xpub)
	xsub := newTestSocket(t, ctx, XSub)
	defer closeSocket(t, xsub)

	endpoint, err := xpub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	if err := xsub.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := xsub.Subscribe(nil); err != nil {
		t.Fatal(err)
	}

	subscription, err := xpub.RecvTimeout(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	frame, ok := subscription.BytesOK()
	if !ok || len(frame) != 1 || frame[0] != 1 {
		t.Fatalf("subscription frame = %v/%v, want [1]/true", frame, ok)
	}
}

func assertRecvString(t *testing.T, socket *Socket, want string) {
	t.Helper()
	msg, err := socket.RecvTimeout(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}
