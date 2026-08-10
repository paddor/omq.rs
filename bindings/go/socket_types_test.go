package omq

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func TestDealerRouterRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	router := newTestSocket(t, ctx, Router)
	defer closeSocket(t, router)
	dealer, err := ctx.Socket(Dealer, Identity([]byte("dealer-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, dealer)

	endpoint, err := router.Bind("inproc://go-dealer-router")
	if err != nil {
		t.Fatal(err)
	}
	if err := dealer.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := dealer.SendTimeout(String("hello"), time.Second); err != nil {
		t.Fatal(err)
	}

	request, err := router.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.Len() != 2 || string(request.Part(0)) != "dealer-1" || string(request.Part(1)) != "hello" {
		t.Fatalf("request parts = %#v", request.Parts())
	}
	if err := router.SendTimeout(Route(request.Part(0), String("world")), time.Second); err != nil {
		t.Fatal(err)
	}
	reply, err := dealer.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := reply.String(); got != "world" {
		t.Fatalf("reply = %q, want world", got)
	}
}

func TestClientServerRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	server := newTestSocket(t, ctx, Server)
	defer closeSocket(t, server)
	client, err := ctx.Socket(Client, Identity([]byte("client-1")))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, client)

	endpoint, err := server.Bind("inproc://go-client-server")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := client.SendTimeout(String("ping"), time.Second); err != nil {
		t.Fatal(err)
	}
	request, err := server.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if request.Len() != 2 || string(request.Part(0)) != "client-1" || string(request.Part(1)) != "ping" {
		t.Fatalf("request parts = %#v", request.Parts())
	}
	if err := server.SendTimeout(Route(request.Part(0), String("pong")), time.Second); err != nil {
		t.Fatal(err)
	}
	reply, err := client.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := reply.String(); got != "pong" {
		t.Fatalf("reply = %q, want pong", got)
	}
}

func TestScatterGatherRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	gather := newTestSocket(t, ctx, Gather)
	defer closeSocket(t, gather)
	scatter := newTestSocket(t, ctx, Scatter)
	defer closeSocket(t, scatter)

	endpoint, err := gather.Bind("inproc://go-scatter-gather")
	if err != nil {
		t.Fatal(err)
	}
	if err := scatter.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := scatter.SendTimeout(String("work"), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := gather.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != "work" {
		t.Fatalf("message = %q, want work", got)
	}
}

func TestChannelAndPairRoundTrip(t *testing.T) {
	testBidirectionalInproc(t, Channel, "inproc://go-channel", "one", "two")
	testBidirectionalInproc(t, Pair, "inproc://go-pair", "x", "y")
}

func TestPeerRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	a, err := ctx.Socket(Peer, Identity([]byte("peer-a")))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, a)
	b, err := ctx.Socket(Peer, Identity([]byte("peer-b")))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, b)

	endpoint, err := a.Bind("inproc://go-peer")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if err := b.SendTimeout(Multipart([]byte("peer-a"), []byte("hello a")), time.Second); err != nil {
		t.Fatal(err)
	}
	got, err := a.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(Multipart([]byte("peer-b"), []byte("hello a"))) {
		t.Fatalf("message = %#v", got.Parts())
	}
	if err := a.SendTimeout(Multipart([]byte("peer-b"), []byte("hello b")), time.Second); err != nil {
		t.Fatal(err)
	}
	got, err = b.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(Multipart([]byte("peer-a"), []byte("hello b"))) {
		t.Fatalf("message = %#v", got.Parts())
	}
}

func TestClientServerMultipleClients(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	server := newTestSocket(t, ctx, Server)
	defer closeSocket(t, server)
	clients := make([]*Socket, 3)
	for i := range clients {
		client, err := ctx.Socket(Client, Identity([]byte{'c', byte('0' + i)}))
		if err != nil {
			t.Fatal(err)
		}
		defer closeSocket(t, client)
		clients[i] = client
	}

	endpoint, err := server.Bind("inproc://go-client-server-many")
	if err != nil {
		t.Fatal(err)
	}
	for _, client := range clients {
		if err := client.Connect(endpoint); err != nil {
			t.Fatal(err)
		}
	}
	for i, client := range clients {
		if err := client.SendTimeout(String("from-"+string(rune('0'+i))), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < len(clients); i++ {
		request, err := server.RecvTimeout(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if request.Len() != 2 {
			t.Fatalf("request parts = %#v", request.Parts())
		}
		reply := Route(request.Route(), String("re:"+string(request.Part(1))))
		if err := server.SendTimeout(reply, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	for _, client := range clients {
		msg, err := client.RecvTimeout(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if got := msg.String(); len(got) < 3 || got[:3] != "re:" {
			t.Fatalf("reply = %q, want re:*", got)
		}
	}
}

func testBidirectionalInproc(t *testing.T, socketType SocketType, endpoint, first, second string) {
	t.Helper()
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	a := newTestSocket(t, ctx, socketType)
	defer closeSocket(t, a)
	b := newTestSocket(t, ctx, socketType)
	defer closeSocket(t, b)

	bound, err := a.Bind(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Connect(bound); err != nil {
		t.Fatal(err)
	}
	if err := a.SendTimeout(String(first), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := b.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != first {
		t.Fatalf("message = %q, want %q", got, first)
	}
	if err := b.SendTimeout(String(second), time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err = a.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.String(); got != second {
		t.Fatalf("message = %q, want %q", got, second)
	}
}

func TestSinglePartSocketsRejectMultipart(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	tests := []struct {
		name       string
		socketType SocketType
		endpoint   string
		message    Message
	}{
		{name: "client", socketType: Client, endpoint: "inproc://go-client-rejects-multipart", message: Multipart([]byte("a"), []byte("b"))},
		{name: "server", socketType: Server, endpoint: "inproc://go-server-requires-routing", message: String("missing-routing-id")},
		{name: "scatter", socketType: Scatter, endpoint: "inproc://go-scatter-rejects-multipart", message: Multipart([]byte("a"), []byte("b"))},
		{name: "channel", socketType: Channel, endpoint: "inproc://go-channel-rejects-multipart", message: Multipart([]byte("a"), []byte("b"))},
		{name: "radio", socketType: Radio, endpoint: "inproc://go-radio-requires-group", message: String("missing-group")},
	}
	for _, test := range tests {
		socket := newTestSocket(t, ctx, test.socketType)
		if _, err := socket.Bind(test.endpoint); err != nil {
			t.Fatal(err)
		}
		err := socket.SendTimeout(test.message, time.Second)
		closeSocket(t, socket)
		if !isProtocolError(err) {
			t.Fatalf("%s send err = %v, want ProtocolError", test.name, err)
		}
	}
}

func TestRadioDishFiltersGroupsAndStringHelpers(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	radio := newTestSocket(t, ctx, Radio)
	defer closeSocket(t, radio)
	dish := newTestSocket(t, ctx, Dish)
	defer closeSocket(t, dish)

	endpoint, err := radio.Bind("inproc://go-radio-dish")
	if err != nil {
		t.Fatal(err)
	}
	if err := dish.JoinString("weather"); err != nil {
		t.Fatal(err)
	}
	if err := dish.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := radio.WaitConnectedTimeout(1, time.Second); err != nil {
		t.Fatal(err)
	}

	if err := radio.SendTimeout(Group("news", []byte("ignored")), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := radio.SendTimeout(Group("weather", []byte("sunny")), time.Second); err != nil {
		t.Fatal(err)
	}
	received, err := dish.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if received.Len() != 2 || string(received.Part(0)) != "weather" || string(received.Part(1)) != "sunny" {
		t.Fatalf("received parts = %#v", received.Parts())
	}

	if err := dish.LeaveString("weather"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := radio.SendTimeout(Group("weather", []byte("rain")), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := dish.RecvTimeout(150 * time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("RecvTimeout err = %v, want ErrTimeout", err)
	}
}

func TestJoinOnWrongSocketTypeIsProtocolError(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	if _, err := pull.Bind("inproc://go-join-wrong-type"); err != nil {
		t.Fatal(err)
	}
	if err := pull.JoinString("g"); !isProtocolError(err) {
		t.Fatalf("Join err = %v, want ProtocolError", err)
	}
}

func TestStreamRawTCPRoundTrip(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	stream := newTestSocket(t, ctx, Stream)
	defer closeSocket(t, stream)
	endpoint, err := stream.Bind("tcp://127.0.0.1:*")
	if err != nil {
		t.Fatal(err)
	}
	address := strings.TrimPrefix(endpoint, "tcp://")
	raw, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if err := raw.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	connected, err := stream.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	identity := connected.Part(0)
	if connected.Len() != 2 || len(identity) == 0 || len(connected.Part(1)) != 0 {
		t.Fatalf("connected parts = %#v", connected.Parts())
	}
	data, err := stream.RecvTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if data.Len() != 2 || string(data.Part(0)) != string(identity) || string(data.Part(1)) != "hello" {
		t.Fatalf("data parts = %#v", data.Parts())
	}
	if err := stream.SendTimeout(Route(identity, String("world")), time.Second); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 5)
	if _, err := raw.Read(reply); err != nil {
		t.Fatal(err)
	}
	if string(reply) != "world" {
		t.Fatalf("raw reply = %q, want world", reply)
	}
}

func TestStreamRejectsNonTCPTransports(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	stream := newTestSocket(t, ctx, Stream)
	defer closeSocket(t, stream)
	if _, err := stream.Bind("inproc://go-stream-inproc"); !isProtocolError(err) {
		t.Fatalf("Bind err = %v, want ProtocolError", err)
	}
}

func isProtocolError(err error) bool {
	var protocol *ProtocolError
	return errors.As(err, &protocol)
}
