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
	identityOption := Identity(identity)
	identity[0] = 'x'
	dealer, err := ctx.Socket(Dealer, identityOption)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, dealer)
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

	options := dealer.Options()
	if !options.Identity.Set || string(options.Identity.Value) != "before" {
		t.Fatalf("identity option = %q/%v, want before/true", options.Identity.Value, options.Identity.Set)
	}
	options.Identity.Value[0] = 'z'
	again := dealer.Options()
	if string(again.Identity.Value) != "before" {
		t.Fatalf("identity option mutated = %q, want before", again.Identity.Value)
	}

	dict := []byte("abcd")
	dictOption := CompressionDict(dict)
	dict[0] = 'z'
	push, err := ctx.Socket(Push, dictOption)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)
	pushOptions := push.Options()
	if !pushOptions.CompressionDict.Set || string(pushOptions.CompressionDict.Value) != "abcd" {
		t.Fatalf("compression dict option = %q/%v, want abcd/true", pushOptions.CompressionDict.Value, pushOptions.CompressionDict.Set)
	}
	pushOptions.CompressionDict.Value[0] = 'y'
	pushAgain := push.Options()
	if string(pushAgain.CompressionDict.Value) != "abcd" {
		t.Fatalf("compression dict option mutated = %q, want abcd", pushAgain.CompressionDict.Value)
	}
}

func TestOptionsSnapshotReportsConfiguredValues(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	socket, err := ctx.Socket(Push,
		SendHWM(7),
		RecvHWM(8),
		Linger(2*time.Millisecond),
		HeartbeatOff(),
		NoHeartbeatTTL(),
		DefaultHeartbeatTimeout(),
		NoHandshakeTimeout(),
		NoMaxMessageSize(),
		PlainClient("alice", "secret"),
		Workload(WorkloadLatency),
		ReconnectExponential(time.Millisecond, 10*time.Millisecond),
		ReconnectStopConnRefused(true),
		MaxPendingHandshakes(4),
		Conflate(true),
		RouterMandatory(true),
		OnMutePolicy(OnMuteDropNewest),
		TCPKeepalive(time.Second, time.Second, 2),
		SendBufferSize(1024),
		RecvBufferSize(2048),
		XPubNoDrop(true),
		CompressionAutoTrain(true),
		CompressionThreshold(64),
		CompressionLevel(1),
		CompressionDict([]byte("abcd")),
		CompressionDictCapacity(4096),
		MaxRecvDictSize(8192),
		CompressionOffloadThreshold(16384),
		LargeMessageThreshold(32768),
		ArenaThreshold(65536),
		TransmitSlotCapacity(131072),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, socket)

	options := socket.Options()
	if !options.SendHWM.Set || options.SendHWM.Value != 7 {
		t.Fatalf("SendHWM = %#v", options.SendHWM)
	}
	if !options.RecvHWM.Set || options.RecvHWM.Value != 8 {
		t.Fatalf("RecvHWM = %#v", options.RecvHWM)
	}
	if !options.Linger.Set || options.Linger.Value != 2*time.Millisecond {
		t.Fatalf("Linger = %#v", options.Linger)
	}
	if !options.HeartbeatOff || !options.NoHeartbeatTTL || !options.DefaultHeartbeatTimeout ||
		!options.NoHandshakeTimeout || !options.NoMaxMessageSize {
		t.Fatalf("duration sentinel options = %#v", options)
	}
	if !options.PlainClient.Set || options.PlainClient.Value.Username != "alice" ||
		options.PlainClient.Value.Password != "secret" {
		t.Fatalf("PlainClient = %#v", options.PlainClient)
	}
	if !options.Workload.Set || options.Workload.Value != WorkloadLatency {
		t.Fatalf("Workload = %#v", options.Workload)
	}
	if !options.Reconnect.Set || options.Reconnect.Value.Mode != "exponential" {
		t.Fatalf("Reconnect = %#v", options.Reconnect)
	}
	if !options.ReconnectStopConnRefused.Set || !options.ReconnectStopConnRefused.Value {
		t.Fatalf("ReconnectStopConnRefused = %#v", options.ReconnectStopConnRefused)
	}
	if !options.MaxPendingHandshakes.Set || options.MaxPendingHandshakes.Value != 4 {
		t.Fatalf("MaxPendingHandshakes = %#v", options.MaxPendingHandshakes)
	}
	if !options.Conflate.Set || !options.Conflate.Value ||
		!options.RouterMandatory.Set || !options.RouterMandatory.Value ||
		!options.XPubNoDrop.Set || !options.XPubNoDrop.Value {
		t.Fatalf("bool options = %#v", options)
	}
	if !options.OnMute.Set || options.OnMute.Value != OnMuteDropNewest {
		t.Fatalf("OnMute = %#v", options.OnMute)
	}
	if !options.TCPKeepalive.Set || options.TCPKeepalive.Value.Mode != "enabled" ||
		options.TCPKeepalive.Value.Count != 2 {
		t.Fatalf("TCPKeepalive = %#v", options.TCPKeepalive)
	}
	if !options.SendBufferSize.Set || options.SendBufferSize.Value != 1024 ||
		!options.RecvBufferSize.Set || options.RecvBufferSize.Value != 2048 {
		t.Fatalf("buffer sizes = %#v/%#v", options.SendBufferSize, options.RecvBufferSize)
	}
	if !options.CompressionAutoTrain.Set || !options.CompressionAutoTrain.Value ||
		!options.CompressionThreshold.Set || options.CompressionThreshold.Value != 64 ||
		!options.CompressionLevel.Set || options.CompressionLevel.Value != 1 ||
		!options.CompressionDict.Set || string(options.CompressionDict.Value) != "abcd" {
		t.Fatalf("compression basics = %#v", options)
	}
	if !options.CompressionDictCapacity.Set || options.CompressionDictCapacity.Value != 4096 ||
		!options.MaxRecvDictSize.Set || options.MaxRecvDictSize.Value != 8192 ||
		!options.CompressionOffloadThreshold.Set || options.CompressionOffloadThreshold.Value != 16384 ||
		!options.LargeMessageThreshold.Set || options.LargeMessageThreshold.Value != 32768 ||
		!options.ArenaThreshold.Set || options.ArenaThreshold.Value != 65536 ||
		!options.TransmitSlotCapacity.Set || options.TransmitSlotCapacity.Value != 131072 {
		t.Fatalf("compression/perf sizes = %#v", options)
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
		PlainServerCredentials(PlainCredential{
			Username: string(make([]byte, zmtpMaxShortStringBytes+1)),
			Password: "secret",
		}),
		PlainServerCredentials(PlainCredential{Username: "has space", Password: "secret"}),
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
