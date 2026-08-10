package omq

import (
	"context"
	"errors"
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
	switch os.Getenv("OMQ_GO_BENCH_PEER") {
	case "push", "push-run":
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

func BenchmarkInprocPushPullRecvInto128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc-into")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	dst := make([]byte, benchPayloadSize)
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
		n, err := pull.RecvInto(runCtx, dst)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(payload) {
			b.Fatalf("payload len = %d", n)
		}
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInprocPushPullRunRecvInto128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc-run")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	dst := make([]byte, benchPayloadSize)
	runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		errCh <- push.Run(runCtx, func(socket *BoundSocket) error {
			for i := 0; i < b.N; i++ {
				if err := socket.Send(runCtx, msg); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	err = pull.Run(runCtx, func(socket *BoundSocket) error {
		for i := 0; i < b.N; i++ {
			n, err := socket.RecvInto(runCtx, dst)
			if err != nil {
				return err
			}
			if n != len(payload) {
				return fmt.Errorf("payload len = %d", n)
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInprocPushPullRunBlockingRecvInto128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc-run-blocking")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	dst := make([]byte, benchPayloadSize)

	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		errCh <- push.Run(context.Background(), func(socket *BoundSocket) error {
			for i := 0; i < b.N; i++ {
				if err := socket.SendBlocking(msg); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	err = pull.Run(context.Background(), func(socket *BoundSocket) error {
		for i := 0; i < b.N; i++ {
			n, err := socket.RecvIntoBlocking(dst)
			if err != nil {
				return err
			}
			if n != len(payload) {
				return fmt.Errorf("payload len = %d", n)
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInprocPushPullRunTryRecvInto128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc-run-try")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)
	dst := make([]byte, benchPayloadSize)

	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		errCh <- push.Run(context.Background(), func(socket *BoundSocket) error {
			for i := 0; i < b.N; i++ {
				for {
					err := socket.TrySend(msg)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrAgain) {
						return err
					}
				}
			}
			return nil
		})
	}()
	err = pull.Run(context.Background(), func(socket *BoundSocket) error {
		for i := 0; i < b.N; i++ {
			for {
				n, err := socket.TryRecvInto(dst)
				if err == nil {
					if n != len(payload) {
						return fmt.Errorf("payload len = %d", n)
					}
					break
				}
				if !errors.Is(err, ErrAgain) {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInprocPushPullRunTryRecvView128B(b *testing.B) {
	ctx := openBenchContext(b)
	defer ctx.Close()

	pull := openBenchSocket(b, ctx, Pull)
	defer pull.Close(context.Background())
	push := openBenchSocket(b, ctx, Push)
	defer push.Close(context.Background())

	endpoint, err := pull.Bind("inproc://bench-inproc-run-view")
	if err != nil {
		b.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		b.Fatal(err)
	}

	payload := make([]byte, benchPayloadSize)
	msg := Bytes(payload)

	b.SetBytes(benchPayloadSize)
	b.ReportAllocs()
	b.ResetTimer()
	errCh := make(chan error, 1)
	go func() {
		errCh <- push.Run(context.Background(), func(socket *BoundSocket) error {
			for i := 0; i < b.N; i++ {
				for {
					err := socket.TrySend(msg)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrAgain) {
						return err
					}
				}
			}
			return nil
		})
	}()
	expectedLen := len(payload)
	checkPayload := func(view []byte) error {
		if len(view) != expectedLen {
			return fmt.Errorf("payload len = %d", len(view))
		}
		return nil
	}
	err = pull.Run(context.Background(), func(socket *BoundSocket) error {
		for i := 0; i < b.N; i++ {
			for {
				err := socket.TryRecvView(checkPayload)
				if err == nil {
					break
				}
				if !errors.Is(err, ErrAgain) {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTCPPushPull128BTwoProcesses(b *testing.B) {
	benchmarkTCPPushPullTwoProcesses(b, false)
}

func BenchmarkTCPPushPullRunRecvInto128BTwoProcesses(b *testing.B) {
	benchmarkTCPPushPullTwoProcesses(b, true)
}

func benchmarkTCPPushPullTwoProcesses(b *testing.B, runFastPath bool) {
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
	peer := "push"
	if runFastPath {
		peer = "push-run"
	}
	cmd.Env = append(os.Environ(),
		"OMQ_GO_BENCH_PEER="+peer,
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
	if runFastPath {
		dst := make([]byte, benchPayloadSize)
		err = pull.Run(runCtx, func(socket *BoundSocket) error {
			for i := 0; i < b.N; i++ {
				n, err := socket.RecvIntoBlocking(dst)
				if err != nil {
					return err
				}
				if n != len(payload) {
					return fmt.Errorf("payload len = %d", n)
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	} else {
		for i := 0; i < b.N; i++ {
			msg, err := pull.Recv(runCtx)
			if err != nil {
				b.Fatal(err)
			}
			if len(msg.Bytes()) != len(payload) {
				b.Fatalf("payload len = %d", len(msg.Bytes()))
			}
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
	ctx, err := Open(Config{IOThreads: 1})
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
	if os.Getenv("OMQ_GO_BENCH_PEER") == "push-run" {
		err := push.Run(runCtx, func(socket *BoundSocket) error {
			for i := 0; i < count; i++ {
				if err := socket.SendBlocking(msg); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		return 0
	}
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
	ctx, err := Open(Config{IOThreads: 1})
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
