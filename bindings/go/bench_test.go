package omq

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const benchPayloadSize = 128

func TestMain(m *testing.M) {
	if os.Getenv("OMQ_GO_BENCH_PEER") == "push" {
		os.Exit(runBenchPushPeer())
	}
	os.Exit(m.Run())
}

func BenchmarkInprocPushPull128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		for i := 0; i < b.N; i++ {
			if err := push.Send(runCtx, msg); err != nil {
				errCh <- err
				return
			}
		}
		errCh <- nil
	}()
	for i := 0; i < b.N; i++ {
		if _, err := pull.Recv(runCtx); err != nil {
			b.Fatal(err)
		}
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTCPPushPull128BTwoProcesses(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	monitor, err := pull.Monitor()
	if err != nil {
		b.Fatal(err)
	}
	defer monitor.Close()

	endpoint, err := pull.Bind("tcp://127.0.0.1:*")
	if err != nil {
		b.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		b.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^$")
	cmd.Env = append(os.Environ(),
		"OMQ_GO_BENCH_PEER=push",
		"OMQ_GO_BENCH_ENDPOINT="+endpoint,
		"OMQ_GO_BENCH_COUNT="+strconv.Itoa(b.N),
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		b.Fatal(err)
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		b.Fatal(err)
	}
	waitBenchHandshake(b, monitor)

	payload := make([]byte, benchPayloadSize)
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := stdin.Write([]byte{1}); err != nil {
		b.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, err := pull.Recv(runCtx)
		if err != nil {
			b.Fatal(err)
		}
		if len(msg.Bytes()) != len(payload) {
			b.Fatalf("payload len = %d", len(msg.Bytes()))
		}
	}
	b.StopTimer()
	if err := cmd.Wait(); err != nil {
		b.Fatal(err)
	}
}

func runBenchPushPeer() int {
	endpoint := os.Getenv("OMQ_GO_BENCH_ENDPOINT")
	count, err := strconv.Atoi(os.Getenv("OMQ_GO_BENCH_COUNT"))
	if err != nil || endpoint == "" {
		fmt.Fprintln(os.Stderr, "invalid bench peer env")
		return 2
	}
	ctx, err := Open(Config{IOThreads: 2})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer ctx.Close()
	push, err := ctx.Socket(Push)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer push.Close(context.Background())
	if err := push.Connect(endpoint); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < count; i++ {
		if err := push.Send(runCtx, msg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	return 0
}

func waitBenchHandshake(b *testing.B, monitor *Monitor) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		event, err := monitor.Recv(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if event.Kind == "HANDSHAKE_SUCCEEDED" {
			return
		}
	}
}

func openBenchContext(b *testing.B) *Context {
	b.Helper()
	ctx, err := Open(Config{IOThreads: 2})
	if err != nil {
		b.Fatal(err)
	}
	return ctx
}

func openBenchSocket(b *testing.B, ctx *Context, socketType SocketType) *Socket {
	b.Helper()
	socket, err := ctx.Socket(socketType)
	if err != nil {
		b.Fatal(err)
	}
	return socket
}
