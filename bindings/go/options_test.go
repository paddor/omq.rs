package omq

import (
	"errors"
	"testing"
	"time"
)

func TestOptionsAcceptBeforeMaterialization(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)

	options := []SocketOption{
		Linger(0),
		LingerForever(),
		Workload(WorkloadLatency),
		DefaultWorkload(),
		ReconnectDisabled(),
		ReconnectInterval(100 * time.Millisecond),
		ReconnectExponential(10*time.Millisecond, time.Second),
		ReconnectStopConnRefused(true),
		SendHWM(10),
		RecvHWM(11),
		HeartbeatInterval(100 * time.Millisecond),
		HeartbeatTTL(3 * time.Second),
		NoHeartbeatTTL(),
		HeartbeatTimeout(5 * time.Second),
		DefaultHeartbeatTimeout(),
		HeartbeatOff(),
		HandshakeTimeout(time.Second),
		MaxPendingHandshakes(8),
		NoMaxMessageSize(),
		Conflate(false),
		RouterMandatory(false),
		OnMutePolicy(OnMuteBlock),
		OnMutePolicy(OnMuteDropNewest),
		OnMutePolicy(OnMuteDropOldest),
		TCPKeepalive(time.Minute, 10*time.Second, 3),
		TCPKeepaliveOff(),
		TCPKeepaliveDefault(),
		SendBufferSize(65_536),
		DefaultSendBufferSize(),
		RecvBufferSize(65_536),
		DefaultRecvBufferSize(),
		CompressionDict([]byte("dict")),
		NoCompressionDict(),
		CompressionAutoTrain(true),
		CompressionThreshold(128),
		CompressionDefaultThreshold(),
		CompressionLevel(1),
		CompressionDefaultLevel(),
		CompressionDictCapacity(2_048),
		DefaultCompressionDictCapacity(),
		MaxRecvDictSize(8_192),
		DefaultMaxRecvDictSize(),
		CompressionOffloadThreshold(8_192),
		NoCompressionOffload(),
		LargeMessageThreshold(4_096),
		DisableLargeMessagePath(),
		ArenaThreshold(65_536),
		DefaultArenaThreshold(),
		TransmitSlotCapacity(2 * 1024 * 1024),
		DefaultTransmitSlotCapacity(),
		XPubNoDrop(false),
	}
	for _, option := range options {
		if err := option(push); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOptionsCopyByteSlices(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	identity := []byte("before")
	dealer, err := ctx.Socket(Dealer, Identity(identity))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, dealer)
	identity[0] = 'x'
	router := newTestSocket(t, ctx, Router)
	defer closeSocket(t, router)

	endpoint, err := router.Bind("inproc://go-options-copy-identity")
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
	if got := string(request.Part(0)); got != "before" {
		t.Fatalf("identity = %q, want before", got)
	}
}

func TestOptionsCannotChangeAfterMaterialization(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)
	if err := push.Connect("tcp://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	if err := Linger(0)(push); !isConfigError(err) {
		t.Fatalf("Linger after connect err = %v, want ConfigError", err)
	}
	if err := RouterMandatory(true)(push); !isConfigError(err) {
		t.Fatalf("RouterMandatory after connect err = %v, want ConfigError", err)
	}
}

func TestOptionsRejectInvalidValues(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)

	invalid := []SocketOption{
		Linger(-time.Millisecond),
		HeartbeatInterval(-time.Millisecond),
		HeartbeatTTL(time.Duration(maxHeartbeatTTLMillis+1) * time.Millisecond),
		HandshakeTimeout(-time.Millisecond),
		MaxMessageSize(-1),
		ReconnectExponential(time.Second, time.Millisecond),
		MaxPendingHandshakes(0),
		TCPKeepalive(time.Second, time.Second, 0),
		SendBufferSize(-1),
		RecvBufferSize(-1),
		CompressionDict(nil),
		CompressionDict(make([]byte, compressionDictMaxBytes+1)),
		CompressionLevel(zstdLevelMax + 1),
		CompressionThreshold(-1),
		CompressionDictCapacity(-1),
		MaxRecvDictSize(-1),
		CompressionOffloadThreshold(-1),
		LargeMessageThreshold(-1),
		ArenaThreshold(-1),
		TransmitSlotCapacity(-1),
		Identity(make([]byte, zmtpMaxShortStringBytes+1)),
		PlainServer(string(make([]byte, zmtpMaxShortStringBytes+1)), "secret"),
		PlainClient("alice", string(make([]byte, zmtpMaxShortStringBytes+1))),
		PlainServerAuth(nil),
		CurveServerAuth(CurveKeypair{}, nil),
	}
	for _, option := range invalid {
		if err := option(pull); !isConfigError(err) {
			t.Fatalf("option err = %v, want ConfigError", err)
		}
	}

	first, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	if err := CurveServer(CurveKeypair{Public: first.Public, Secret: second.Secret})(pull); !isConfigError(err) {
		t.Fatalf("CurveServer mismatched keypair err = %v, want ConfigError", err)
	}
}

func isConfigError(err error) bool {
	var config *ConfigError
	return errors.As(err, &config)
}
