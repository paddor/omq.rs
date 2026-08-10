package omq

import (
	"context"
	"testing"
	"time"
)

func TestProxyCapturePauseResumeAndTerminate(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)

	frontend := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, frontend)
	backend := newTestSocket(t, ctx, Push)
	defer closeSocket(t, backend)
	source := newTestSocket(t, ctx, Push)
	defer closeSocket(t, source)
	sink := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, sink)
	capture := newTestSocket(t, ctx, Push)
	defer closeSocket(t, capture)
	captureSink := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, captureSink)
	controlProxy := newTestSocket(t, ctx, Rep)
	defer closeSocket(t, controlProxy)
	controlClient := newTestSocket(t, ctx, Req)
	defer closeSocket(t, controlClient)

	frontendEndpoint, err := frontend.Bind("inproc://go-proxy-control-fe")
	if err != nil {
		t.Fatal(err)
	}
	backendEndpoint, err := sink.Bind("inproc://go-proxy-control-be")
	if err != nil {
		t.Fatal(err)
	}
	captureEndpoint, err := captureSink.Bind("inproc://go-proxy-control-capture")
	if err != nil {
		t.Fatal(err)
	}
	controlEndpoint, err := controlProxy.Bind("inproc://go-proxy-control")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Connect(frontendEndpoint); err != nil {
		t.Fatal(err)
	}
	if err := backend.Connect(backendEndpoint); err != nil {
		t.Fatal(err)
	}
	if err := capture.Connect(captureEndpoint); err != nil {
		t.Fatal(err)
	}
	if err := controlClient.Connect(controlEndpoint); err != nil {
		t.Fatal(err)
	}

	proxyCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proxyErr := make(chan error, 1)
	go func() {
		proxyErr <- Proxy(proxyCtx, frontend, backend, ProxyOptions{
			Capture: capture,
			Control: controlProxy,
		})
	}()

	sendProxyCommand(t, controlClient, "PAUSE", true)
	if err := source.SendTimeout(String("held"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.RecvTimeout(100 * time.Millisecond); err == nil {
		t.Fatal("proxy forwarded while paused")
	}
	sendProxyCommand(t, controlClient, "RESUME", true)
	assertRecvString(t, sink, "held")
	assertRecvString(t, captureSink, "held")

	sendProxyCommand(t, controlClient, "TERMINATE", false)
	select {
	case err := <-proxyErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy did not terminate")
	}
}

func sendProxyCommand(t *testing.T, control *Socket, command string, expectReply bool) {
	t.Helper()
	if err := control.SendTimeout(String(command), time.Second); err != nil {
		t.Fatal(err)
	}
	if !expectReply {
		return
	}
	if _, err := control.RecvTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
}
