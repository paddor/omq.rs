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

	soakResourceWarmup     = 10 * time.Minute
	soakResourceWindow     = 5 * time.Minute
	soakResourceMinSamples = 12
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
	baseline := readSoakResources()
	limits := readSoakResourceLimits()

	omqCtx, err := Open(Config{IOThreads: workers, RingSize: 4096})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	start := time.Now()
	resources := newSoakResourceTracker(start, baseline, limits)

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
			current := resources.sample(t, elapsed)
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
				current.heapBytes/1_048_576,
				current.rssBytes/1_048_576,
				current.fdCount,
			)
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
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	resources.assertFinal(t, time.Since(start))
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
	wg.Go(func() {
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
	})
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
		senders.Go(func() {
			payload := []byte{idx, 0, 0, 0, 0, 0, 0, 0}
			for senderCtx.Err() == nil {
				if err := socket.SendTimeout(Bytes(payload), soakSendTimeout); err != nil &&
					!errors.Is(err, ErrTimeout) && !errors.Is(err, ErrAgain) {
					return
				}
				payload[1]++
			}
		})
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

func TestResourceSlopePerSecond(t *testing.T) {
	start := time.Unix(0, 0)
	samples := []soakResourceSample{
		{at: start, value: 10},
		{at: start.Add(time.Second), value: 20},
		{at: start.Add(2 * time.Second), value: 30},
	}
	slope, ok := slopePerSecond(samples)
	if !ok {
		t.Fatal("slopePerSecond returned !ok")
	}
	if slope < 9.999 || slope > 10.001 {
		t.Fatalf("slope = %f, want 10", slope)
	}
}

func TestResourceLiveGrowthDetectsSustainedRSSGrowth(t *testing.T) {
	start := time.Unix(0, 0)
	var samples []soakResourceSample
	for seconds := 0; seconds <= 1_200; seconds += 20 {
		samples = append(samples, soakResourceSample{
			at:    start.Add(time.Duration(seconds) * time.Second),
			value: uint64(seconds) * 1_048_576,
		})
	}
	err := liveGrowthError("RSS", start, samples, 128, 8*1_048_576)
	if err == nil {
		t.Fatal("liveGrowthError returned nil")
	}
}

func TestResourceLiveGrowthIgnoresPlateau(t *testing.T) {
	start := time.Unix(0, 0)
	var samples []soakResourceSample
	for seconds := 0; seconds <= 1_200; seconds += 20 {
		value := uint64(min(seconds, 600)) * 1_048_576
		samples = append(samples, soakResourceSample{
			at:    start.Add(time.Duration(seconds) * time.Second),
			value: value,
		})
	}
	err := liveGrowthError("RSS", start, samples, 128, 8*1_048_576)
	if err != nil {
		t.Fatal(err)
	}
}

func TestResourceLiveFDGrowthDetectsSustainedGrowth(t *testing.T) {
	start := time.Unix(0, 0)
	var samples []soakResourceSample
	for seconds := 0; seconds <= 1_200; seconds += 20 {
		samples = append(samples, soakResourceSample{
			at:    start.Add(time.Duration(seconds) * time.Second),
			value: uint64(10 + seconds/2),
		})
	}
	err := liveFDGrowthError(start, samples, 0.05, 32)
	if err == nil {
		t.Fatal("liveFDGrowthError returned nil")
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

func nonNegativeInt64Config(names []string, fallback int64) int64 {
	value := int64Config(names, fallback)
	if value < 0 {
		return fallback
	}
	return value
}

func mibConfig(names []string, fallback int64) uint64 {
	return uint64(nonNegativeInt64Config(names, fallback)) * 1_048_576
}

func float64Config(names []string, fallback float64) float64 {
	for _, name := range names {
		value := os.Getenv(name)
		if value == "" {
			continue
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

type soakResourceLimits struct {
	heapGrowthBytes    uint64
	rssGrowthBytes     uint64
	fdGrowth           uint64
	finalFDGrowth      uint64
	heapSlopeKiBPerSec float64
	rssSlopeKiBPerSec  float64
	fdSlopePerSec      float64
	heapSlopeMinGrowth uint64
	rssSlopeMinGrowth  uint64
	fdSlopeMinGrowth   uint64
}

type soakResources struct {
	heapBytes uint64
	rssBytes  uint64
	fdCount   uint64
}

type soakResourceSample struct {
	at    time.Time
	value uint64
}

type soakResourceTracker struct {
	started  time.Time
	baseline soakResources
	limits   soakResourceLimits
	heap     []soakResourceSample
	rss      []soakResourceSample
	fds      []soakResourceSample
}

func readSoakResourceLimits() soakResourceLimits {
	// RSS has normal delayed native warm-up under churn. Keep the live
	// slope gate focused on sustained growth large enough to matter.
	return soakResourceLimits{
		heapGrowthBytes: mibConfig(
			[]string{"OMQ_GO_SOAK_MAX_HEAP_GROWTH_MB"}, 128,
		),
		rssGrowthBytes: mibConfig(
			[]string{"OMQ_GO_SOAK_MAX_RSS_GROWTH_MB"}, 512,
		),
		fdGrowth: uint64(nonNegativeInt64Config(
			[]string{"OMQ_GO_SOAK_MAX_FD_GROWTH"}, 128,
		)),
		finalFDGrowth: uint64(nonNegativeInt64Config(
			[]string{"OMQ_GO_SOAK_MAX_FINAL_FD_GROWTH"}, 16,
		)),
		heapSlopeKiBPerSec: float64Config(
			[]string{"OMQ_GO_SOAK_HEAP_SLOPE_LIMIT_KIB_S"}, 512,
		),
		rssSlopeKiBPerSec: float64Config(
			[]string{"OMQ_GO_SOAK_RSS_SLOPE_LIMIT_KIB_S"}, 1024,
		),
		fdSlopePerSec: float64Config(
			[]string{"OMQ_GO_SOAK_FD_SLOPE_LIMIT_PER_SEC"}, 0.05,
		),
		heapSlopeMinGrowth: mibConfig(
			[]string{"OMQ_GO_SOAK_HEAP_SLOPE_MIN_GROWTH_MB"}, 16,
		),
		rssSlopeMinGrowth: mibConfig(
			[]string{"OMQ_GO_SOAK_RSS_SLOPE_MIN_GROWTH_MB"}, 128,
		),
		fdSlopeMinGrowth: uint64(nonNegativeInt64Config(
			[]string{"OMQ_GO_SOAK_FD_SLOPE_MIN_GROWTH"}, 32,
		)),
	}
}

func newSoakResourceTracker(
	started time.Time,
	baseline soakResources,
	limits soakResourceLimits,
) *soakResourceTracker {
	tracker := &soakResourceTracker{
		started:  started,
		baseline: baseline,
		limits:   limits,
	}
	tracker.appendSample(started, baseline)
	return tracker
}

func (r *soakResourceTracker) sample(t *testing.T, elapsed time.Duration) soakResources {
	t.Helper()
	current := readSoakResources()
	r.appendSample(time.Now(), current)
	assertSoakResources(t, elapsed, r.baseline, current, r.limits)
	if err := liveGrowthError(
		"heap",
		r.started,
		r.heap,
		r.limits.heapSlopeKiBPerSec,
		r.limits.heapSlopeMinGrowth,
	); err != nil {
		t.Fatal(err)
	}
	if err := liveGrowthError(
		"RSS",
		r.started,
		r.rss,
		r.limits.rssSlopeKiBPerSec,
		r.limits.rssSlopeMinGrowth,
	); err != nil {
		t.Fatal(err)
	}
	if err := liveFDGrowthError(
		r.started,
		r.fds,
		r.limits.fdSlopePerSec,
		r.limits.fdSlopeMinGrowth,
	); err != nil {
		t.Fatal(err)
	}
	return current
}

func (r *soakResourceTracker) assertFinal(t *testing.T, elapsed time.Duration) {
	t.Helper()
	current := r.sample(t, elapsed)
	t.Logf(
		"[go-soak] final resources heap=%dMB rss=%dMB fds=%d",
		current.heapBytes/1_048_576,
		current.rssBytes/1_048_576,
		current.fdCount,
	)
	r.logSlope(t, "heap", r.heap, 1024, "KiB")
	r.logSlope(t, "RSS", r.rss, 1024, "KiB")
	r.logSlope(t, "FD", r.fds, 1, "FDs")
	if current.fdCount > r.baseline.fdCount+r.limits.finalFDGrowth {
		t.Fatalf(
			"final FD growth exceeded limit: baseline=%d current=%d limit=%d",
			r.baseline.fdCount,
			current.fdCount,
			r.limits.finalFDGrowth,
		)
	}
}

func (r *soakResourceTracker) appendSample(at time.Time, resources soakResources) {
	r.heap = append(r.heap, soakResourceSample{at: at, value: resources.heapBytes})
	r.rss = append(r.rss, soakResourceSample{at: at, value: resources.rssBytes})
	r.fds = append(r.fds, soakResourceSample{at: at, value: resources.fdCount})
}

func (r *soakResourceTracker) logSlope(
	t *testing.T,
	metric string,
	samples []soakResourceSample,
	scale float64,
	unit string,
) {
	t.Helper()
	if len(samples) < 2 {
		return
	}
	warmup := len(samples) / 5
	postWarmup := samples[warmup:]
	slope, ok := slopePerSecond(postWarmup)
	if !ok {
		return
	}
	minValue, maxValue := sampleRange(postWarmup)
	t.Logf(
		"[go-soak] %s range %.1f..%.1f %s slope %.3f %s/s",
		metric,
		float64(minValue)/scale,
		float64(maxValue)/scale,
		unit,
		slope/scale,
		unit,
	)
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

func liveGrowthError(
	metric string,
	started time.Time,
	samples []soakResourceSample,
	slopeLimitKiBPerSec float64,
	minGrowthBytes uint64,
) error {
	window, ok := liveGrowthWindow(started, samples)
	if !ok {
		return nil
	}
	current := window[len(window)-1].value
	growth := saturatingSub(current, window[0].value)
	if growth < minGrowthBytes {
		return nil
	}
	slope, ok := slopePerSecond(window)
	if !ok {
		return nil
	}
	slopeKiBPerSec := slope / 1024
	if slopeKiBPerSec <= slopeLimitKiBPerSec {
		return nil
	}
	return fmt.Errorf(
		"live %s growth detected: slope %.1f KiB/s over %.0fs, growth %.1f MiB, current %.1f MiB, limit %.1f KiB/s",
		metric,
		slopeKiBPerSec,
		soakResourceWindow.Seconds(),
		float64(growth)/1_048_576,
		float64(current)/1_048_576,
		slopeLimitKiBPerSec,
	)
}

func liveFDGrowthError(
	started time.Time,
	samples []soakResourceSample,
	slopeLimitPerSec float64,
	minGrowth uint64,
) error {
	window, ok := liveGrowthWindow(started, samples)
	if !ok {
		return nil
	}
	current := window[len(window)-1].value
	growth := saturatingSub(current, window[0].value)
	if growth < minGrowth {
		return nil
	}
	slope, ok := slopePerSecond(window)
	if !ok || slope <= slopeLimitPerSec {
		return nil
	}
	minFD, maxFD := sampleRange(window)
	return fmt.Errorf(
		"live FD growth detected: slope %.4f FDs/s over %.0fs, range %d..%d, limit %.4f FDs/s",
		slope,
		soakResourceWindow.Seconds(),
		minFD,
		maxFD,
		slopeLimitPerSec,
	)
}

func liveGrowthWindow(
	started time.Time,
	samples []soakResourceSample,
) ([]soakResourceSample, bool) {
	if len(samples) < soakResourceMinSamples {
		return nil, false
	}
	now := samples[len(samples)-1].at
	if now.Sub(started) < soakResourceWarmup+soakResourceWindow {
		return nil, false
	}
	windowStart := now.Add(-soakResourceWindow)
	first := 0
	for i, sample := range samples {
		if !sample.at.Before(windowStart) {
			first = i
			break
		}
	}
	window := samples[first:]
	if len(window) < soakResourceMinSamples {
		return nil, false
	}
	return window, true
}

func slopePerSecond(samples []soakResourceSample) (float64, bool) {
	if len(samples) < 2 {
		return 0, false
	}
	first := samples[0].at
	elapsed := samples[len(samples)-1].at.Sub(first).Seconds()
	if elapsed < 1 {
		return 0, false
	}
	n := float64(len(samples))
	var sumX, sumY, sumXY, sumXX float64
	for _, sample := range samples {
		x := sample.at.Sub(first).Seconds()
		y := float64(sample.value)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}
	denom := n*sumXX - sumX*sumX
	if denom == 0 {
		return 0, false
	}
	return (n*sumXY - sumX*sumY) / denom, true
}

func sampleRange(samples []soakResourceSample) (uint64, uint64) {
	if len(samples) == 0 {
		return 0, 0
	}
	minValue := samples[0].value
	maxValue := samples[0].value
	for _, sample := range samples[1:] {
		minValue = min(minValue, sample.value)
		maxValue = max(maxValue, sample.value)
	}
	return minValue, maxValue
}

func saturatingSub(value uint64, other uint64) uint64 {
	if value < other {
		return 0
	}
	return value - other
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
