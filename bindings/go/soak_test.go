package omq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	soakRecvTimeout    = 200 * time.Millisecond
	soakSendTimeout    = 500 * time.Millisecond
	soakConnectTimeout = 5 * time.Second
	soakReportInterval = 10 * time.Second
)

type soakCounters struct {
	tcpMessages         atomic.Uint64
	curveMessages       atomic.Uint64
	compressionMessages atomic.Uint64
	inprocMessages      atomic.Uint64
	pollerMessages      atomic.Uint64
	pubSubMessages      atomic.Uint64
	contextCycles       atomic.Uint64
	monitorEvents       atomic.Uint64
}

type soakState struct {
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	failure atomic.Value
}

func TestSoakMixedWorkloads(t *testing.T) {
	if !soakEnabled() {
		t.Skip("set OMQ_GO_SOAK=1 to run Go soak")
	}

	duration := soakDuration()
	workers := soakWorkers()
	oldProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(oldProcs)

	runCtx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	state := &soakState{ctx: runCtx, cancel: cancel}
	counters := &soakCounters{}
	limits := readSoakResourceLimits()
	baseline := readSoakResources()

	omqCtx, err := Open(Config{IOThreads: workers, RingSize: 4096})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	start := time.Now()

	tcpEndpoint, tcpPull := startSoakPull(t, omqCtx, "tcp://127.0.0.1:*", nil)
	tcpMonitor, err := tcpPull.Monitor()
	if err != nil {
		t.Fatal(err)
	}
	startWorker(&wg, state, "tcp-pull", func(ctx context.Context) error {
		return soakDrainMessages(ctx, tcpPull, &counters.tcpMessages)
	})
	startWorker(&wg, state, "tcp-monitor", func(ctx context.Context) error {
		defer tcpMonitor.Close()
		return soakDrainMonitor(ctx, tcpMonitor, &counters.monitorEvents)
	})

	serverKey, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := GenerateCurveKeypair()
	if err != nil {
		t.Fatal(err)
	}
	curveEndpoint, curvePull := startSoakPull(t, omqCtx, "tcp://127.0.0.1:*", []SocketOption{
		CurveServerAuth(serverKey, func(peer PeerInfo) bool {
			return peer.Mechanism == "CURVE" && peer.PublicKey == clientKey.Public
		}),
	})
	startWorker(&wg, state, "curve-pull", func(ctx context.Context) error {
		return soakDrainMessages(ctx, curvePull, &counters.curveMessages)
	})

	churnWorkers := max(1, workers/3)
	for i := 0; i < churnWorkers; i++ {
		workerID := i
		startWorker(&wg, state, fmt.Sprintf("tcp-churn-%d", workerID), func(ctx context.Context) error {
			return soakChurnPush(ctx, omqCtx, tcpEndpoint, workerID, nil)
		})
		startWorker(&wg, state, fmt.Sprintf("curve-churn-%d", workerID), func(ctx context.Context) error {
			return soakChurnPush(ctx, omqCtx, curveEndpoint, workerID, []SocketOption{
				CurveClient(clientKey, serverKey.Public),
			})
		})
	}

	startWorker(&wg, state, "lz4-compression", func(ctx context.Context) error {
		return soakCompressionPair(ctx, omqCtx, "lz4+tcp://127.0.0.1:*", nil, counters)
	})
	startWorker(&wg, state, "zstd-compression", func(ctx context.Context) error {
		return soakCompressionPair(ctx, omqCtx, "zstd+tcp://127.0.0.1:*", zstdTestDict, counters)
	})
	startWorker(&wg, state, "inproc-req-rep", func(ctx context.Context) error {
		return soakInprocReqRep(ctx, omqCtx, counters)
	})
	startWorker(&wg, state, "poller-fanin", func(ctx context.Context) error {
		return soakPollerFanIn(ctx, omqCtx, max(2, min(workers/2, 6)), counters)
	})
	startWorker(&wg, state, "pub-sub-churn", func(ctx context.Context) error {
		return soakPubSubChurn(ctx, omqCtx, counters)
	})
	startWorker(&wg, state, "context-churn", func(ctx context.Context) error {
		return soakContextChurn(ctx, counters)
	})

	ticker := time.NewTicker(soakReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			goto done
		case <-ticker.C:
			elapsed := time.Since(start)
			resources := readSoakResources()
			t.Logf(
				"[go-soak] %.0fs tcp=%d curve=%d compression=%d inproc=%d poller=%d pubsub=%d contexts=%d monitor=%d heap=%dMB rss=%dMB fds=%d",
				elapsed.Seconds(),
				counters.tcpMessages.Load(),
				counters.curveMessages.Load(),
				counters.compressionMessages.Load(),
				counters.inprocMessages.Load(),
				counters.pollerMessages.Load(),
				counters.pubSubMessages.Load(),
				counters.contextCycles.Load(),
				counters.monitorEvents.Load(),
				resources.heapBytes/1_048_576,
				resources.rssBytes/1_048_576,
				resources.fdCount,
			)
			assertSoakResources(t, elapsed, baseline, resources, limits)
			if err := state.err(); err != nil {
				cancel()
			}
		}
	}

done:
	cancel()
	waitForSoakWorkers(t, &wg)
	closeSoakSocket(tcpPull)
	closeSoakSocket(curvePull)
	if err := omqCtx.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	assertSoakResources(t, time.Since(start), baseline, readSoakResources(), limits)
	if err := state.err(); err != nil {
		t.Fatal(err)
	}
	assertSoakProgress(t, counters)
}

func startSoakPull(t *testing.T, ctx *Context, endpoint string, extra []SocketOption) (string, *Socket) {
	t.Helper()
	opts := append([]SocketOption{}, soakRecvOptions()...)
	opts = append(opts, extra...)
	pull, err := ctx.Socket(Pull, opts...)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := pull.Bind(endpoint)
	if err != nil {
		closeSoakSocket(pull)
		t.Fatal(err)
	}
	return bound, pull
}

func startWorker(wg *sync.WaitGroup, state *soakState, name string, fn func(context.Context) error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				state.fail(fmt.Errorf("%s: panic: %v", name, recovered))
			}
		}()
		err := fn(state.ctx)
		if err == nil || (state.ctx.Err() != nil && soakStopError(err)) {
			return
		}
		state.fail(fmt.Errorf("%s: %w", name, err))
	}()
}

func (s *soakState) fail(err error) {
	if err == nil {
		return
	}
	s.once.Do(func() {
		s.failure.Store(err)
		s.cancel()
	})
}

func (s *soakState) err() error {
	value := s.failure.Load()
	if value == nil {
		return nil
	}
	return value.(error)
}

func soakDrainMessages(ctx context.Context, socket *Socket, counter *atomic.Uint64) error {
	for ctx.Err() == nil {
		_, err := socket.RecvTimeout(soakRecvTimeout)
		switch {
		case err == nil:
			counter.Add(1)
		case errors.Is(err, ErrTimeout), errors.Is(err, ErrAgain):
		default:
			return err
		}
	}
	return errFromContext(ctx)
}

func soakDrainMonitor(ctx context.Context, monitor *Monitor, counter *atomic.Uint64) error {
	for ctx.Err() == nil {
		_, err := monitor.RecvTimeout(soakRecvTimeout)
		switch {
		case err == nil:
			counter.Add(1)
		case errors.Is(err, ErrTimeout), errors.Is(err, ErrAgain):
		default:
			return err
		}
	}
	return errFromContext(ctx)
}

func soakChurnPush(ctx context.Context, shared *Context, endpoint string, workerID int, extra []SocketOption) error {
	seq := uint64(0)
	for ctx.Err() == nil {
		opts := append([]SocketOption{}, soakSendOptions()...)
		opts = append(opts, extra...)
		push, err := shared.Socket(Push, opts...)
		if err != nil {
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakSocket(push)
			return err
		}
		if _, err := push.WaitConnectedTimeout(1, soakConnectTimeout); err != nil {
			closeSoakSocket(push)
			if ctx.Err() != nil || errors.Is(err, ErrTimeout) {
				continue
			}
			return err
		}
		payload := soakPayload(fmt.Sprintf("churn-%d", workerID), seq, 256)
		for i := 0; i < 32 && ctx.Err() == nil; i++ {
			payload[0] = byte(i)
			if err := push.SendTimeout(Bytes(payload), soakSendTimeout); err != nil &&
				!errors.Is(err, ErrTimeout) && !errors.Is(err, ErrAgain) {
				closeSoakSocket(push)
				return err
			}
			seq++
		}
		closeSoakSocket(push)
	}
	return errFromContext(ctx)
}

func soakCompressionPair(ctx context.Context, shared *Context, endpoint string, dict []byte, counters *soakCounters) error {
	pullOpts := append([]SocketOption{}, soakRecvOptions()...)
	pushOpts := append([]SocketOption{}, soakSendOptions()...)
	pullOpts = append(pullOpts, CompressionAutoTrain(true), CompressionDictCapacity(4096))
	pushOpts = append(pushOpts,
		CompressionAutoTrain(true),
		CompressionDictCapacity(4096),
		CompressionThreshold(32),
		CompressionLevel(1),
	)
	if dict != nil {
		pullOpts = append(pullOpts, CompressionDict(dict))
		pushOpts = append(pushOpts, CompressionDict(dict))
	}
	pull, err := shared.Socket(Pull, pullOpts...)
	if err != nil {
		return err
	}
	defer closeSoakSocket(pull)
	push, err := shared.Socket(Push, pushOpts...)
	if err != nil {
		return err
	}
	defer closeSoakSocket(push)
	bound, err := pull.Bind(endpoint)
	if err != nil {
		return err
	}
	if err := push.Connect(bound); err != nil {
		return err
	}
	if _, err := push.WaitConnectedTimeout(1, soakConnectTimeout); err != nil {
		return err
	}
	var seq uint64
	for ctx.Err() == nil {
		payload := soakPayload(endpoint, seq, 1024)
		if err := push.SendTimeout(Bytes(payload), 5*time.Second); err != nil {
			return err
		}
		msg, err := pull.RecvTimeout(5 * time.Second)
		if err != nil {
			return err
		}
		if !bytes.Equal(msg.Bytes(), payload) {
			return fmt.Errorf("compression payload mismatch on %s", endpoint)
		}
		counters.compressionMessages.Add(1)
		seq++
	}
	return errFromContext(ctx)
}

func soakInprocReqRep(ctx context.Context, shared *Context, counters *soakCounters) error {
	rep, err := shared.Socket(Rep, soakRecvOptions()...)
	if err != nil {
		return err
	}
	defer closeSoakSocket(rep)
	req, err := shared.Socket(Req, soakSendOptions()...)
	if err != nil {
		return err
	}
	defer closeSoakSocket(req)
	endpoint, err := rep.Bind(fmt.Sprintf("inproc://go-soak-req-rep-%d", os.Getpid()))
	if err != nil {
		return err
	}
	if err := req.Connect(endpoint); err != nil {
		return err
	}
	var seq uint64
	for ctx.Err() == nil {
		payload := fmt.Sprintf("req-%d", seq)
		if err := req.SendTimeout(String(payload), 5*time.Second); err != nil {
			return err
		}
		msg, err := rep.RecvTimeout(5 * time.Second)
		if err != nil {
			return err
		}
		if msg.String() != payload {
			return fmt.Errorf("inproc request mismatch: %q", msg.String())
		}
		if err := rep.SendTimeout(String("ok"), 5*time.Second); err != nil {
			return err
		}
		reply, err := req.RecvTimeout(5 * time.Second)
		if err != nil {
			return err
		}
		if reply.String() != "ok" {
			return fmt.Errorf("inproc reply mismatch: %q", reply.String())
		}
		counters.inprocMessages.Add(1)
		seq++
	}
	return errFromContext(ctx)
}

func soakPollerFanIn(ctx context.Context, shared *Context, channels int, counters *soakCounters) error {
	pulls := make([]*Socket, 0, channels)
	pushes := make([]*Socket, 0, channels)
	for i := 0; i < channels; i++ {
		pull, err := shared.Socket(Pull, soakRecvOptions()...)
		if err != nil {
			return err
		}
		push, err := shared.Socket(Push, soakSendOptions()...)
		if err != nil {
			closeSoakSocket(pull)
			return err
		}
		endpoint, err := pull.Bind(fmt.Sprintf("inproc://go-soak-poller-%d-%d", os.Getpid(), i))
		if err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			return err
		}
		pulls = append(pulls, pull)
		pushes = append(pushes, push)
	}
	defer func() {
		for _, push := range pushes {
			closeSoakSocket(push)
		}
		for _, pull := range pulls {
			closeSoakSocket(pull)
		}
	}()

	var senders sync.WaitGroup
	senderCtx, stopSenders := context.WithCancel(ctx)
	defer func() {
		stopSenders()
		senders.Wait()
	}()

	for i, push := range pushes {
		idx := byte(i)
		socket := push
		senders.Add(1)
		go func() {
			defer senders.Done()
			payload := []byte{idx, 0, 0, 0, 0, 0, 0, 0}
			for senderCtx.Err() == nil {
				if err := socket.SendTimeout(Bytes(payload), soakSendTimeout); err != nil &&
					!errors.Is(err, ErrTimeout) && !errors.Is(err, ErrAgain) {
					return
				}
				payload[1]++
			}
		}()
	}

	poller, err := NewPoller(pulls...)
	if err != nil {
		return err
	}
	seen := make([]uint64, channels)
	for ctx.Err() == nil {
		event, err := poller.RecvTimeout(soakRecvTimeout)
		switch {
		case err == nil:
			msg := event.Message.Bytes()
			if len(msg) == 0 || int(msg[0]) >= channels {
				return fmt.Errorf("bad poller payload: %x", msg)
			}
			seen[int(msg[0])]++
			counters.pollerMessages.Add(1)
		case errors.Is(err, ErrTimeout), errors.Is(err, ErrAgain):
		default:
			return err
		}
	}
	for i, count := range seen {
		if count == 0 {
			return fmt.Errorf("poller channel %d made no progress", i)
		}
	}
	return errFromContext(ctx)
}

func soakPubSubChurn(ctx context.Context, shared *Context, counters *soakCounters) error {
	topics := []string{"fast.", "slow.", "all.", "rare."}
	pub, err := shared.Socket(Pub,
		Linger(0),
		SendHWM(8192),
		OnMutePolicy(OnMuteDropNewest),
		Workload(WorkloadThroughput),
	)
	if err != nil {
		return err
	}
	defer closeSoakSocket(pub)
	endpoint, err := pub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		return err
	}

	var subs []*Socket
	defer func() {
		for _, sub := range subs {
			closeSoakSocket(sub)
		}
	}()

	var seq uint64
	lastChurn := time.Now()
	for ctx.Err() == nil {
		topic := topics[int(seq)%len(topics)]
		for i := 0; i < 128; i++ {
			if err := pub.SendTimeout(String(fmt.Sprintf("%s%d", topic, seq)), soakSendTimeout); err != nil &&
				!errors.Is(err, ErrTimeout) && !errors.Is(err, ErrAgain) {
				return err
			}
			seq++
		}
		for _, sub := range subs {
			for {
				_, err := sub.TryRecv()
				if err == nil {
					counters.pubSubMessages.Add(1)
					continue
				}
				if errors.Is(err, ErrAgain) {
					break
				}
				return err
			}
		}
		if time.Since(lastChurn) >= 500*time.Millisecond {
			lastChurn = time.Now()
			if len(subs) > 0 {
				closeSoakSocket(subs[0])
				copy(subs, subs[1:])
				subs = subs[:len(subs)-1]
			}
			if len(subs) < 10 {
				sub, err := shared.Socket(Sub, soakRecvOptions()...)
				if err != nil {
					return err
				}
				if err := sub.Connect(endpoint); err != nil {
					closeSoakSocket(sub)
					return err
				}
				if err := sub.SubscribeString(topics[len(subs)%len(topics)]); err != nil {
					closeSoakSocket(sub)
					return err
				}
				subs = append(subs, sub)
			}
		}
	}
	return errFromContext(ctx)
}

func soakContextChurn(ctx context.Context, counters *soakCounters) error {
	var seq uint64
	for ctx.Err() == nil {
		churnCtx, err := Open(Config{IOThreads: 1})
		if err != nil {
			return err
		}
		pull, err := churnCtx.Socket(Pull, Linger(0))
		if err != nil {
			_ = churnCtx.Close()
			return err
		}
		push, err := churnCtx.Socket(Push, Linger(0))
		if err != nil {
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return err
		}
		endpoint, err := pull.Bind(fmt.Sprintf("inproc://go-soak-context-%d-%d", os.Getpid(), seq))
		if err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return err
		}
		if err := push.SendTimeout(String("x"), time.Second); err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return err
		}
		msg, err := pull.RecvTimeout(time.Second)
		if err != nil {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return err
		}
		if msg.String() != "x" {
			closeSoakSocket(push)
			closeSoakSocket(pull)
			_ = churnCtx.Close()
			return fmt.Errorf("context churn payload mismatch: %q", msg.String())
		}
		closeSoakSocket(push)
		closeSoakSocket(pull)
		if err := churnCtx.CloseContext(context.Background()); err != nil {
			return err
		}
		counters.contextCycles.Add(1)
		seq++
	}
	return errFromContext(ctx)
}

func waitForSoakWorkers(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("soak workers did not stop within 30s")
	}
}

func assertSoakProgress(t *testing.T, counters *soakCounters) {
	t.Helper()
	checks := []struct {
		name  string
		count uint64
	}{
		{"tcp", counters.tcpMessages.Load()},
		{"curve", counters.curveMessages.Load()},
		{"compression", counters.compressionMessages.Load()},
		{"inproc", counters.inprocMessages.Load()},
		{"poller", counters.pollerMessages.Load()},
		{"context-churn", counters.contextCycles.Load()},
	}
	for _, check := range checks {
		if check.count == 0 {
			t.Fatalf("%s soak made no progress", check.name)
		}
	}
}

func soakSendOptions() []SocketOption {
	return []SocketOption{
		Linger(0),
		SendHWM(8192),
		ReconnectInterval(20 * time.Millisecond),
		Workload(WorkloadThroughput),
	}
}

func soakRecvOptions() []SocketOption {
	return []SocketOption{
		Linger(0),
		RecvHWM(8192),
		HeartbeatInterval(10 * time.Second),
		Workload(WorkloadThroughput),
	}
}

func soakPayload(kind string, seq uint64, size int) []byte {
	head := fmt.Sprintf(`{"kind":"%s","seq":%d,"pad":"`, kind, seq)
	tail := `"}`
	if len(head)+len(tail) > size {
		panic("soak payload size too small")
	}
	return []byte(head + strings.Repeat("x", size-len(head)-len(tail)) + tail)
}

func soakEnabled() bool {
	return os.Getenv("OMQ_GO_SOAK") == "1"
}

func soakDuration() time.Duration {
	secs := int64Config([]string{"OMQ_GO_SOAK_DURATION_SECS", "OMQ_SOAK_DURATION_SECS"}, 60)
	if secs < 5 {
		secs = 5
	}
	return time.Duration(secs) * time.Second
}

func soakWorkers() int {
	workers := int(int64Config([]string{"OMQ_GO_SOAK_WORKERS"}, int64(runtime.NumCPU())))
	if workers < 1 {
		return 1
	}
	return workers
}

func int64Config(names []string, fallback int64) int64 {
	for _, name := range names {
		value := os.Getenv(name)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

type soakResourceLimits struct {
	heapGrowthBytes uint64
	rssGrowthBytes  uint64
	fdGrowth        uint64
}

type soakResources struct {
	heapBytes uint64
	rssBytes  uint64
	fdCount   uint64
}

func readSoakResourceLimits() soakResourceLimits {
	return soakResourceLimits{
		heapGrowthBytes: uint64(int64Config([]string{"OMQ_GO_SOAK_MAX_HEAP_GROWTH_MB"}, 384)) * 1_048_576,
		rssGrowthBytes:  uint64(int64Config([]string{"OMQ_GO_SOAK_MAX_RSS_GROWTH_MB"}, 768)) * 1_048_576,
		fdGrowth:        uint64(int64Config([]string{"OMQ_GO_SOAK_MAX_FD_GROWTH"}, 1024)),
	}
}

func readSoakResources() soakResources {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss := readRSSBytes()
	if rss == 0 {
		rss = stats.Sys
	}
	return soakResources{
		heapBytes: stats.Alloc,
		rssBytes:  rss,
		fdCount:   readFDCount(),
	}
}

func assertSoakResources(
	t *testing.T,
	elapsed time.Duration,
	baseline soakResources,
	current soakResources,
	limits soakResourceLimits,
) {
	t.Helper()
	if elapsed < 20*time.Second {
		return
	}
	if current.heapBytes > baseline.heapBytes+limits.heapGrowthBytes {
		t.Fatalf("heap growth exceeded limit: baseline=%d current=%d limit=%d",
			baseline.heapBytes, current.heapBytes, limits.heapGrowthBytes)
	}
	if current.rssBytes > baseline.rssBytes+limits.rssGrowthBytes {
		t.Fatalf("RSS growth exceeded limit: baseline=%d current=%d limit=%d",
			baseline.rssBytes, current.rssBytes, limits.rssGrowthBytes)
	}
	if current.fdCount > baseline.fdCount+limits.fdGrowth {
		t.Fatalf("FD growth exceeded limit: baseline=%d current=%d limit=%d",
			baseline.fdCount, current.fdCount, limits.fdGrowth)
	}
}

func readRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

func readFDCount() uint64 {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return uint64(len(entries))
}

func closeSoakSocket(socket *Socket) {
	if socket != nil {
		_ = socket.Close(context.Background())
	}
}

func soakStopError(err error) bool {
	return errors.Is(err, ErrCanceled) ||
		errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
