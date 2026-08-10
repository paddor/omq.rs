package omq

import (
	"testing"
	"time"
)

func TestPlainServerAuthCallbackTCP(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	seen := make(chan PeerInfo, 4)
	pull, err := ctx.Socket(Pull, PlainServerAuth(func(peer PeerInfo) bool {
		select {
		case seen <- peer:
		default:
		}
		return peer.Mechanism == "PLAIN" && peer.Username == "alice" && peer.Password == "secret"
	}))
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
	if err := push.SendTimeout(String("plain-callback"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "plain-callback" {
		t.Fatalf("message = %q, want plain-callback", got)
	}
	assertAuthPeer(t, seen, "PLAIN", "", "alice", "secret")
}

func TestCurveServerAuthCallbackTCP(t *testing.T) {
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

	seen := make(chan PeerInfo, 4)
	pull, err := ctx.Socket(Pull, CurveServerAuth(serverKeypair, func(peer PeerInfo) bool {
		select {
		case seen <- peer:
		default:
		}
		return peer.Mechanism == "CURVE" && peer.PublicKey == clientKeypair.Public
	}))
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
	if err := push.SendTimeout(String("curve-callback"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := pull.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "curve-callback" {
		t.Fatalf("message = %q, want curve-callback", got)
	}
	assertAuthPeer(t, seen, "CURVE", clientKeypair.Public, "", "")
}

func assertAuthPeer(
	t *testing.T,
	seen <-chan PeerInfo,
	mechanism string,
	publicKey string,
	username string,
	password string,
) {
	t.Helper()
	select {
	case peer := <-seen:
		if peer.Mechanism != mechanism || peer.PublicKey != publicKey ||
			peer.Username != username || peer.Password != password {
			t.Fatalf("peer = %#v", peer)
		}
	case <-time.After(time.Second):
		t.Fatal("auth callback was not called")
	}
}
