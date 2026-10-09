package omq

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Gates for blocking wait paths. A path that sleeps 50 us to 1 ms between
// readiness checks needs far longer than these budgets.
const (
	wakeupRounds   = 1000
	wakeupWarmup   = 100
	wakeupBudget   = 600 * time.Millisecond
	wakeupAttempts = 3
	wakeupTimeout  = 5 * time.Second
)

func bestOf(t *testing.T, run func(t *testing.T) time.Duration) time.Duration {
	t.Helper()
	best := time.Duration(1<<63 - 1)
	for range wakeupAttempts {
		best = min(best, run(t))
	}
	return best
}

func reqRepRoundTrips(t *testing.T) time.Duration {
	t.Helper()
	ctx := openTestContext(t)
	defer closeContext(t, ctx)
	rep := newTestSocket(t, ctx, Rep)
	defer closeSocket(t, rep)
	req := newTestSocket(t, ctx, Req)
	defer closeSocket(t, req)

	endpoint, err := rep.Bind("tcp://127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := req.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		for range wakeupRounds {
			msg, err := rep.RecvTimeout(wakeupTimeout)
			if err != nil {
				served <- err
				return
			}
			if err := rep.SendTimeout(msg, wakeupTimeout); err != nil {
				served <- err
				return
			}
		}
		served <- nil
	}()
	var started time.Time
	for i := range wakeupRounds {
		if i == wakeupWarmup {
			started = time.Now()
		}
		if err := req.SendTimeout(String("x"), wakeupTimeout); err != nil {
			t.Fatal(err)
		}
		if _, err := req.RecvTimeout(wakeupTimeout); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	return elapsed
}

func TestReqRepTimedWaitsWakePromptly(t *testing.T) {
	if elapsed := bestOf(t, reqRepRoundTrips); elapsed > wakeupBudget {
		t.Fatalf("%d round trips took %v", wakeupRounds-wakeupWarmup, elapsed)
	}
}

func mutedSendRounds(t *testing.T) time.Duration {
	t.Helper()
	ctx := openTestContext(t)
	defer closeContext(t, ctx)
	pull, err := ctx.Socket(Pull, RecvHWM(1))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, pull)
	push, err := ctx.Socket(Push, SendHWM(1))
	if err != nil {
		t.Fatal(err)
	}
	defer closeSocket(t, push)

	endpoint, err := pull.Bind("inproc://go-wakeup-muted-send")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}
	// Fill the queue so each round's first send starts muted. Receivers
	// release queue space in batches, so each round drains the depth.
	depth := 0
	for {
		err := push.TrySend(String("x"))
		if errors.Is(err, ErrAgain) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		depth++
	}
	rounds := wakeupRounds / 2
	goRound := make(chan struct{}, rounds)
	consumed := make(chan error, 1)
	go func() {
		for range rounds {
			<-goRound
			for range depth {
				if _, err := pull.RecvTimeout(wakeupTimeout); err != nil {
					consumed <- err
					return
				}
			}
		}
		consumed <- nil
	}()
	started := time.Now()
	for range rounds {
		goRound <- struct{}{}
		for range depth {
			sendCtx, cancel := context.WithTimeout(context.Background(), wakeupTimeout)
			err := push.Send(sendCtx, String("x"))
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	elapsed := time.Since(started)
	if err := <-consumed; err != nil {
		t.Fatal(err)
	}
	return elapsed
}

func TestMutedSendWakesPromptly(t *testing.T) {
	if elapsed := bestOf(t, mutedSendRounds); elapsed > wakeupBudget/2 {
		t.Fatalf("%d muted send rounds took %v", wakeupRounds/2, elapsed)
	}
}

func TestMutedSendHonorsContext(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)
	push := newTestSocket(t, ctx, Push)
	defer closeSocket(t, push)
	if _, err := push.Bind("inproc://go-wakeup-muted-cancel"); err != nil {
		t.Fatal(err)
	}
	// Bind-side PUSH without a peer mutes until the context ends.
	sendCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := push.Send(sendCtx, String("lost")); !errors.Is(err, ErrTimeout) {
		t.Fatalf("Send err = %v, want ErrTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled send returned after %v", elapsed)
	}
}

func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func TestIdleSendRingDoesNotPoll(t *testing.T) {
	ctx := openTestContext(t)
	defer closeContext(t, ctx)
	pull := newTestSocket(t, ctx, Pull)
	defer closeSocket(t, pull)
	push := newTestSocket(t, ctx, Push)
	endpoint, err := pull.Bind("inproc://go-wakeup-idle-ring")
	if err != nil {
		t.Fatal(err)
	}
	if err := push.Connect(endpoint); err != nil {
		t.Fatal(err)
	}

	idle := 300 * time.Millisecond
	var burned time.Duration
	err = push.Run(context.Background(), func(bound *BoundSocket) error {
		if err := bound.SendBlocking(String("x")); err != nil {
			return err
		}
		if _, err := pull.RecvTimeout(wakeupTimeout); err != nil {
			return err
		}
		// The ring worker exists now; idle must not wake it on a timer.
		before := cpuTime(t)
		time.Sleep(idle)
		burned = cpuTime(t) - before
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	closeSocket(t, push)
	if burned > idle/20 {
		t.Fatalf("idle send ring burned %v CPU in %v", burned, idle)
	}
}

// timerWaitAllowlist lists the only timer-based waits. Monitor events and
// peer/subscription counts have no native wakeup; data paths must park.
var timerWaitAllowlist = map[string]int{
	"monitor.go": 1, // Monitor.Recv
	"native.go":  2, // waitRetry definition and its timer
	"socket.go":  2, // WaitConnected, WaitSubscribed
}

func TestTimerWaitsStayOffDataPaths(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`time\.Sleep\(|time\.NewTimer\(|time\.After\(|time\.Tick|waitRetry\(`)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := len(pattern.FindAll(source, -1)), timerWaitAllowlist[file]; got != want {
			t.Errorf("%s has %d timer waits, allowlist has %d", file, got, want)
		}
	}
}
