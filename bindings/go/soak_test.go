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
	soakCloseTimeout   = 5 * time.Second
	soakReportInterval = 10 * time.Second

	soakResourceWarmup     = 10 * time.Minute
	soakResourceWindow     = 5 * time.Minute
	soakResourceMinSamples = 12
)

type soakCounters struct {
	tcpMessages          atomic.Uint64
	curveMessages        atomic.Uint64
	compressionMessages  atomic.Uint64
	inprocMessages       atomic.Uint64
	pollerMessages       atomic.Uint64
	pubSubMessages       atomic.Uint64
	protocolMessages     atomic.Uint64
	contextCycles        atomic.Uint64
	monitorEvents        atomic.Uint64
	tcpLifecycle         soakLifecycleCounters
	curveLifecycle       soakLifecycleCounters
	compressionLifecycle soakLifecycleCounters
	inprocLifecycle      soakLifecycleCounters
	pollerLifecycle      soakLifecycleCounters
	pubSubLifecycle      soakLifecycleCounters
	protocolLifecycle    soakLifecycleCounters
	contextLifecycle     soakLifecycleCounters
}

type soakLifecycleCounters struct {
	socketsCreated  atomic.Uint64
	socketsClosed   atomic.Uint64
	contextsCreated atomic.Uint64
	contextsClosed  atomic.Uint64
}

type soakLifecycleSnapshot struct {
	socketsCreated  uint64
	socketsClosed   uint64
	contextsCreated uint64
	contextsClosed  uint64
}

type soakState struct {
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	failure atomic.Value
}

type soakScenarios struct {
	selected map[string]bool
}

var allSoakScenarios = []string{
	"tcp",
	"curve",
	"compression",
	"inproc",
	"poller",
	"pubsub",
	"protocol-mix",
	"context-churn",
}

func TestSoakMixedWorkloads(t *testing.T) {
	if !soakEnabled() {
		t.Skip("set OMQ_GO_SOAK=1 to run Go soak")
	}

	duration := soakDuration()
	workers := soakWorkers()
	scenarios := readSoakScenarios(t)
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

	var tcpEndpoint string
	var tcpPull *Socket
	if scenarios.enabled("tcp") {
		tcpEndpoint, tcpPull = startSoakPull(t, omqCtx, counters, "tcp", "tcp://127.0.0.1:*", nil)
		startWorker(&wg, state, "tcp-pull", func(ctx context.Context) error {
			return soakDrainMessages(ctx, tcpPull, &counters.tcpMessages)
		})
		tcpMonitor, err := tcpPull.Monitor()
		if err != nil {
			t.Fatal(err)
		}
		startWorker(&wg, state, "tcp-monitor", func(ctx context.Context) error {
			defer tcpMonitor.Close()
			return soakDrainMonitor(ctx, tcpMonitor, &counters.monitorEvents)
		})
	}

	var curveEndpoint string
	var curvePull *Socket
	var serverKey CurveKeypair
	var clientKey CurveKeypair
	if scenarios.enabled("curve") {
		var err error
		serverKey, err = GenerateCurveKeypair()
		if err != nil {
			t.Fatal(err)
		}
		clientKey, err = GenerateCurveKeypair()
		if err != nil {
			t.Fatal(err)
		}
		curveEndpoint, curvePull = startSoakPull(t, omqCtx, counters, "curve", "tcp://127.0.0.1:*", []SocketOption{
			CurveServerAuth(serverKey, func(peer PeerInfo) bool {
				return peer.Mechanism == "CURVE" && peer.PublicKey == clientKey.Public
			}),
		})
		startWorker(&wg, state, "curve-pull", func(ctx context.Context) error {
			return soakDrainMessages(ctx, curvePull, &counters.curveMessages)
		})
	}

	churnWorkers := max(1, workers/3)
	if scenarios.enabled("tcp") {
		for i := 0; i < churnWorkers; i++ {
			workerID := i
			startWorker(&wg, state, fmt.Sprintf("tcp-churn-%d", workerID), func(ctx context.Context) error {
				return soakChurnPush(ctx, omqCtx, counters, "tcp", tcpEndpoint, workerID, nil)
			})
		}
	}
	if scenarios.enabled("curve") {
		for i := 0; i < churnWorkers; i++ {
			workerID := i
			startWorker(&wg, state, fmt.Sprintf("curve-churn-%d", workerID), func(ctx context.Context) error {
				return soakChurnPush(ctx, omqCtx, counters, "curve", curveEndpoint, workerID, []SocketOption{
					CurveClient(clientKey, serverKey.Public),
				})
			})
		}
	}

	if scenarios.enabled("compression") {
		startWorker(&wg, state, "lz4-compression", func(ctx context.Context) error {
			return soakCompressionPair(ctx, omqCtx, "lz4+tcp://127.0.0.1:*", nil, counters)
		})
		startWorker(&wg, state, "zstd-compression", func(ctx context.Context) error {
			return soakCompressionPair(ctx, omqCtx, "zstd+tcp://127.0.0.1:*", zstdTestDict, counters)
		})
	}
	if scenarios.enabled("inproc") {
		startWorker(&wg, state, "inproc-req-rep", func(ctx context.Context) error {
			return soakInprocReqRep(ctx, omqCtx, counters)
		})
	}
	if scenarios.enabled("poller") {
		startWorker(&wg, state, "poller-fanin", func(ctx context.Context) error {
			return soakPollerFanIn(ctx, omqCtx, max(2, min(workers/2, 6)), counters)
		})
	}
	if scenarios.enabled("pubsub") {
		startWorker(&wg, state, "pub-sub-churn", func(ctx context.Context) error {
			return soakPubSubChurn(ctx, omqCtx, counters)
		})
	}
	if scenarios.enabled("protocol-mix") {
		startWorker(&wg, state, "protocol-mix", func(ctx context.Context) error {
			return soakProtocolMix(ctx, omqCtx, counters)
		})
	}
	if scenarios.enabled("context-churn") {
		startWorker(&wg, state, "context-churn", func(ctx context.Context) error {
			return soakContextChurn(ctx, counters)
		})
	}

	ticker := time.NewTicker(soakReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			goto done
		case <-ticker.C:
			elapsed := time.Since(start)
			current := resources.sample()
			t.Logf(
				"[go-soak] %.0fs tcp=%d curve=%d compression=%d inproc=%d poller=%d pubsub=%d protocol=%d contexts=%d monitor=%d heap=%dMB rss=%dMB fds=%d goroutines=%d threads=%d cgo=%d",
				elapsed.Seconds(),
				counters.tcpMessages.Load(),
				counters.curveMessages.Load(),
				counters.compressionMessages.Load(),
				counters.inprocMessages.Load(),
				counters.pollerMessages.Load(),
				counters.pubSubMessages.Load(),
				counters.protocolMessages.Load(),
				counters.contextCycles.Load(),
				counters.monitorEvents.Load(),
				current.heapBytes/1_048_576,
				current.rssBytes/1_048_576,
				current.fdCount,
				current.goroutines,
				current.threads,
				current.cgoCalls,
			)
			logSoakResourceDetails(t, current)
			logSoakNativeStats(t, current.native)
			logSoakLifecycle(t, counters)
			resources.assertLive(t, elapsed, current)
			if err := state.err(); err != nil {
				cancel()
			}
		}
	}

done:
	cancel()
	waitForSoakWorkers(t, &wg)
	if tcpPull != nil {
		closeSoakScenarioSocket(tcpPull, counters, "tcp")
	}
	if curvePull != nil {
		closeSoakScenarioSocket(curvePull, counters, "curve")
	}
	if err := closeSoakContext(omqCtx); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	resources.assertFinal(t, time.Since(start))
	logSoakLifecycle(t, counters)
	if err := state.err(); err != nil {
		t.Fatal(err)
	}
	assertSoakProgress(t, scenarios, counters)
}

func startSoakPull(
	t *testing.T,
	ctx *Context,
	counters *soakCounters,
	scenario string,
	endpoint string,
	extra []SocketOption,
) (string, *Socket) {
	t.Helper()
	opts := append([]SocketOption{}, soakRecvOptions()...)
	opts = append(opts, extra...)
	pull, err := soakNewSocket(ctx, counters, scenario, Pull, opts...)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := pull.Bind(endpoint)
	if err != nil {
		closeSoakScenarioSocket(pull, counters, scenario)
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
		case soakMonitorLagged(err):
		default:
			return err
		}
	}
	return errFromContext(ctx)
}

func soakMonitorLagged(err error) bool {
	var config *ConfigError
	return errors.As(err, &config) &&
		strings.HasPrefix(config.Err, "monitor lagged behind; missed ")
}

func soakChurnPush(
	ctx context.Context,
	shared *Context,
	counters *soakCounters,
	scenario string,
	endpoint string,
	workerID int,
	extra []SocketOption,
) error {
	seq := uint64(0)
	for ctx.Err() == nil {
		opts := append([]SocketOption{}, soakSendOptions()...)
		opts = append(opts, extra...)
		push, err := soakNewSocket(shared, counters, scenario, Push, opts...)
		if err != nil {
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakScenarioSocket(push, counters, scenario)
			return err
		}
		if _, err := push.WaitConnectedTimeout(1, soakConnectTimeout); err != nil {
			closeSoakScenarioSocket(push, counters, scenario)
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
				closeSoakScenarioSocket(push, counters, scenario)
				return err
			}
			seq++
		}
		closeSoakScenarioSocket(push, counters, scenario)
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
	pull, err := soakNewSocket(shared, counters, "compression", Pull, pullOpts...)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(pull, counters, "compression")
	push, err := soakNewSocket(shared, counters, "compression", Push, pushOpts...)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(push, counters, "compression")
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
	rep, err := soakNewSocket(shared, counters, "inproc", Rep, soakRecvOptions()...)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(rep, counters, "inproc")
	req, err := soakNewSocket(shared, counters, "inproc", Req, soakSendOptions()...)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(req, counters, "inproc")
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

func soakProtocolMix(ctx context.Context, shared *Context, counters *soakCounters) error {
	newSocket := func(socketType SocketType) (*Socket, error) {
		return soakNewSocket(shared, counters, "protocol-mix", socketType, Linger(0), SendHWM(128), RecvHWM(128))
	}
	pull, err := newSocket(Pull)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(pull, counters, "protocol-mix")
	push, err := newSocket(Push)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(push, counters, "protocol-mix")
	ipcEndpoint, err := pull.Bind(fmt.Sprintf("ipc://@go-soak-protocol-%d", os.Getpid()))
	if err != nil {
		return err
	}
	if err := push.Connect(ipcEndpoint); err != nil {
		return err
	}

	rep, err := newSocket(Rep)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(rep, counters, "protocol-mix")
	req, err := newSocket(Req)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(req, counters, "protocol-mix")
	reqEndpoint, err := rep.Bind("tcp://127.0.0.1:0")
	if err != nil {
		return err
	}
	if err := req.Connect(reqEndpoint); err != nil {
		return err
	}

	left, err := newSocket(Pair)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(left, counters, "protocol-mix")
	right, err := newSocket(Pair)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(right, counters, "protocol-mix")
	pairEndpoint, err := left.Bind("tcp://127.0.0.1:0")
	if err != nil {
		return err
	}
	if err := right.Connect(pairEndpoint); err != nil {
		return err
	}

	large := bytes.Repeat([]byte{0xa5}, 256*1024)
	var seq uint64
	for ctx.Err() == nil {
		sequence := []byte(strconv.FormatUint(seq, 10))
		payload := sequence
		if seq%64 == 0 {
			payload = large
		}
		want := Multipart(sequence, payload)
		if err := push.SendTimeout(want, 5*time.Second); err != nil {
			return err
		}
		got, err := pull.RecvTimeout(5 * time.Second)
		if err != nil {
			return err
		}
		if !got.Equal(want) {
			return fmt.Errorf("IPC multipart mismatch at %d", seq)
		}

		if err := req.SendTimeout(String("request"), 5*time.Second); err != nil {
			return err
		}
		if _, err := rep.RecvTimeout(5 * time.Second); err != nil {
			return err
		}
		if err := rep.SendTimeout(String("reply"), 5*time.Second); err != nil {
			return err
		}
		if reply, err := req.RecvTimeout(5 * time.Second); err != nil || reply.String() != "reply" {
			return fmt.Errorf("REQ/REP mismatch: %v", err)
		}

		pairMessage := Bytes(sequence)
		if err := left.SendTimeout(pairMessage, 5*time.Second); err != nil {
			return err
		}
		if got, err := right.RecvTimeout(5 * time.Second); err != nil || !got.Equal(pairMessage) {
			return fmt.Errorf("PAIR forward mismatch: %v", err)
		}
		if err := right.SendTimeout(pairMessage, 5*time.Second); err != nil {
			return err
		}
		if got, err := left.RecvTimeout(5 * time.Second); err != nil || !got.Equal(pairMessage) {
			return fmt.Errorf("PAIR reverse mismatch: %v", err)
		}
		counters.protocolMessages.Add(4)
		seq++
	}
	return errFromContext(ctx)
}

func soakPollerFanIn(ctx context.Context, shared *Context, channels int, counters *soakCounters) error {
	pulls := make([]*Socket, 0, channels)
	pushes := make([]*Socket, 0, channels)
	for i := 0; i < channels; i++ {
		pull, err := soakNewSocket(shared, counters, "poller", Pull, soakRecvOptions()...)
		if err != nil {
			return err
		}
		push, err := soakNewSocket(shared, counters, "poller", Push, soakSendOptions()...)
		if err != nil {
			closeSoakScenarioSocket(pull, counters, "poller")
			return err
		}
		endpoint, err := pull.Bind(fmt.Sprintf("inproc://go-soak-poller-%d-%d", os.Getpid(), i))
		if err != nil {
			closeSoakScenarioSocket(push, counters, "poller")
			closeSoakScenarioSocket(pull, counters, "poller")
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakScenarioSocket(push, counters, "poller")
			closeSoakScenarioSocket(pull, counters, "poller")
			return err
		}
		pulls = append(pulls, pull)
		pushes = append(pushes, push)
	}
	defer func() {
		for _, push := range pushes {
			closeSoakScenarioSocket(push, counters, "poller")
		}
		for _, pull := range pulls {
			closeSoakScenarioSocket(pull, counters, "poller")
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
	pub, err := soakNewSocket(shared, counters, "pubsub", Pub,
		Linger(0),
		SendHWM(8192),
		OnMutePolicy(OnMuteDropNewest),
		Workload(WorkloadThroughput),
	)
	if err != nil {
		return err
	}
	defer closeSoakScenarioSocket(pub, counters, "pubsub")
	endpoint, err := pub.Bind("tcp://127.0.0.1:*")
	if err != nil {
		return err
	}

	var subs []*Socket
	defer func() {
		for _, sub := range subs {
			closeSoakScenarioSocket(sub, counters, "pubsub")
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
				closeSoakScenarioSocket(subs[0], counters, "pubsub")
				copy(subs, subs[1:])
				subs = subs[:len(subs)-1]
			}
			if len(subs) < 10 {
				sub, err := soakNewSocket(shared, counters, "pubsub", Sub, soakRecvOptions()...)
				if err != nil {
					return err
				}
				if err := sub.Connect(endpoint); err != nil {
					closeSoakScenarioSocket(sub, counters, "pubsub")
					return err
				}
				if err := sub.SubscribeString(topics[len(subs)%len(topics)]); err != nil {
					closeSoakScenarioSocket(sub, counters, "pubsub")
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
		counters.scenarioContextCreated("context-churn")
		pull, err := soakNewSocket(churnCtx, counters, "context-churn", Pull, Linger(0))
		if err != nil {
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		push, err := soakNewSocket(churnCtx, counters, "context-churn", Push, Linger(0))
		if err != nil {
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		endpoint, err := pull.Bind(fmt.Sprintf("inproc://go-soak-context-%d-%d", os.Getpid(), seq))
		if err != nil {
			closeSoakScenarioSocket(push, counters, "context-churn")
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		if err := push.Connect(endpoint); err != nil {
			closeSoakScenarioSocket(push, counters, "context-churn")
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		if err := push.SendTimeout(String("x"), time.Second); err != nil {
			closeSoakScenarioSocket(push, counters, "context-churn")
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		msg, err := pull.RecvTimeout(time.Second)
		if err != nil {
			closeSoakScenarioSocket(push, counters, "context-churn")
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return err
		}
		if msg.String() != "x" {
			closeSoakScenarioSocket(push, counters, "context-churn")
			closeSoakScenarioSocket(pull, counters, "context-churn")
			_ = closeSoakContext(churnCtx)
			counters.scenarioContextClosed("context-churn")
			return fmt.Errorf("context churn payload mismatch: %q", msg.String())
		}
		closeSoakScenarioSocket(push, counters, "context-churn")
		closeSoakScenarioSocket(pull, counters, "context-churn")
		if err := closeSoakContext(churnCtx); err != nil {
			counters.scenarioContextClosed("context-churn")
			if ctx.Err() != nil && errors.Is(err, ErrTimeout) {
				return errFromContext(ctx)
			}
			return err
		}
		counters.scenarioContextClosed("context-churn")
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

func assertSoakProgress(t *testing.T, scenarios soakScenarios, counters *soakCounters) {
	t.Helper()
	checks := []struct {
		name  string
		count uint64
	}{}
	if scenarios.enabled("tcp") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"tcp", counters.tcpMessages.Load()})
	}
	if scenarios.enabled("curve") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"curve", counters.curveMessages.Load()})
	}
	if scenarios.enabled("compression") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"compression", counters.compressionMessages.Load()})
	}
	if scenarios.enabled("inproc") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"inproc", counters.inprocMessages.Load()})
	}
	if scenarios.enabled("poller") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"poller", counters.pollerMessages.Load()})
	}
	if scenarios.enabled("pubsub") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"pubsub", counters.pubSubMessages.Load()})
	}
	if scenarios.enabled("protocol-mix") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"protocol-mix", counters.protocolMessages.Load()})
	}
	if scenarios.enabled("context-churn") {
		checks = append(checks, struct {
			name  string
			count uint64
		}{"context-churn", counters.contextCycles.Load()})
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

func TestResourceTailGrowthDetectsDrift(t *testing.T) {
	start := time.Unix(0, 0)
	var samples []soakResourceSample
	for i := 0; i < 100; i++ {
		value := uint64(100 + i*4)
		samples = append(samples, soakResourceSample{
			at:    start.Add(time.Duration(i) * time.Second),
			value: value * 1_048_576,
		})
	}
	baseline, tailMax, ok := tailGrowthWindow(samples)
	if !ok {
		t.Fatal("tailGrowthWindow returned !ok")
	}
	growthPercent := float64(saturatingSub(tailMax, baseline)) / float64(baseline) * 100
	if growthPercent <= 25 {
		t.Fatalf("growthPercent = %f, want > 25", growthPercent)
	}
}

func TestResourceTailGrowthIgnoresPlateau(t *testing.T) {
	start := time.Unix(0, 0)
	var samples []soakResourceSample
	for i := 0; i < 100; i++ {
		value := uint64(min(100+i*4, 220))
		samples = append(samples, soakResourceSample{
			at:    start.Add(time.Duration(i) * time.Second),
			value: value * 1_048_576,
		})
	}
	baseline, tailMax, ok := tailGrowthWindow(samples)
	if !ok {
		t.Fatal("tailGrowthWindow returned !ok")
	}
	growthPercent := float64(saturatingSub(tailMax, baseline)) / float64(baseline) * 100
	if growthPercent > 25 {
		t.Fatalf("growthPercent = %f, want <= 25", growthPercent)
	}
}

func TestResourceRSSResidualIgnoresReleasedHighWater(t *testing.T) {
	baseline := uint64(124 * 1_048_576)
	tailMax := uint64(259 * 1_048_576)
	final := uint64(124 * 1_048_576)
	if rssResidualLeak(baseline, tailMax, final, 25, 128*1_048_576) {
		t.Fatal("rssResidualLeak reported released high-water as a leak")
	}
}

func TestResourceRSSResidualDetectsRetainedTail(t *testing.T) {
	baseline := uint64(124 * 1_048_576)
	tailMax := uint64(259 * 1_048_576)
	final := uint64(258 * 1_048_576)
	if !rssResidualLeak(baseline, tailMax, final, 25, 128*1_048_576) {
		t.Fatal("rssResidualLeak missed retained RSS growth")
	}
}

func TestResourceParsesProcStatus(t *testing.T) {
	status := parseProcStatus("Name:\tgo.test\nVmRSS:\t  1234 kB\nVmData:\t  5678 kB\nThreads:\t42\n")
	if status.vmRSSBytes != 1234*1024 {
		t.Fatalf("vmRSSBytes = %d", status.vmRSSBytes)
	}
	if status.vmDataBytes != 5678*1024 {
		t.Fatalf("vmDataBytes = %d", status.vmDataBytes)
	}
	if status.threads != 42 {
		t.Fatalf("threads = %d", status.threads)
	}
}

func TestResourceParsesProcSmapsRollup(t *testing.T) {
	smaps := parseProcSmapsRollup("Rss: 100 kB\nAnonymous: 80 kB\nPrivate_Dirty: 60 kB\n")
	if smaps.rssBytes != 100*1024 {
		t.Fatalf("rssBytes = %d", smaps.rssBytes)
	}
	if smaps.anonymousBytes != 80*1024 {
		t.Fatalf("anonymousBytes = %d", smaps.anonymousBytes)
	}
	if smaps.privateDirtyBytes != 60*1024 {
		t.Fatalf("privateDirtyBytes = %d", smaps.privateDirtyBytes)
	}
}

func TestResourceNativeStatsLiveGrowth(t *testing.T) {
	baseline := nativeStats{socketsLive: 1, sendRingsLive: 2}
	current := nativeStats{socketsLive: 2, sendRingsLive: 2}
	if got := current.liveGrowthSince(baseline); got != "sockets=1" {
		t.Fatalf("liveGrowthSince = %q", got)
	}
	if got := baseline.liveGrowthSince(current); got != "" {
		t.Fatalf("liveGrowthSince with no growth = %q", got)
	}
}

func (c *soakCounters) scenario(name string) *soakLifecycleCounters {
	switch name {
	case "tcp":
		return &c.tcpLifecycle
	case "curve":
		return &c.curveLifecycle
	case "compression":
		return &c.compressionLifecycle
	case "inproc":
		return &c.inprocLifecycle
	case "poller":
		return &c.pollerLifecycle
	case "pubsub":
		return &c.pubSubLifecycle
	case "protocol-mix":
		return &c.protocolLifecycle
	case "context-churn":
		return &c.contextLifecycle
	default:
		return nil
	}
}

func (c *soakCounters) scenarioSocketCreated(name string) {
	if lifecycle := c.scenario(name); lifecycle != nil {
		lifecycle.socketsCreated.Add(1)
	}
}

func (c *soakCounters) scenarioSocketClosed(name string) {
	if lifecycle := c.scenario(name); lifecycle != nil {
		lifecycle.socketsClosed.Add(1)
	}
}

func (c *soakCounters) scenarioContextCreated(name string) {
	if lifecycle := c.scenario(name); lifecycle != nil {
		lifecycle.contextsCreated.Add(1)
	}
}

func (c *soakCounters) scenarioContextClosed(name string) {
	if lifecycle := c.scenario(name); lifecycle != nil {
		lifecycle.contextsClosed.Add(1)
	}
}

func (c *soakCounters) lifecycleString(name string) string {
	lifecycle := c.scenario(name)
	if lifecycle == nil {
		return "s=0/0 c=0/0"
	}
	snapshot := lifecycle.snapshot()
	return fmt.Sprintf(
		"s=%d/%d c=%d/%d",
		snapshot.socketsCreated,
		snapshot.socketsClosed,
		snapshot.contextsCreated,
		snapshot.contextsClosed,
	)
}

func (c *soakLifecycleCounters) snapshot() soakLifecycleSnapshot {
	return soakLifecycleSnapshot{
		socketsCreated:  c.socketsCreated.Load(),
		socketsClosed:   c.socketsClosed.Load(),
		contextsCreated: c.contextsCreated.Load(),
		contextsClosed:  c.contextsClosed.Load(),
	}
}

func (stats nativeStats) liveGrowthSince(baseline nativeStats) string {
	parts := make([]string, 0, 6)
	add := func(name string, current uint64, base uint64) {
		if current > base {
			parts = append(parts, fmt.Sprintf("%s=%d", name, current-base))
		}
	}
	add("contexts", stats.contextsLive, baseline.contextsLive)
	add("sockets", stats.socketsLive, baseline.socketsLive)
	add("monitors", stats.monitorsLive, baseline.monitorsLive)
	add("send_rings", stats.sendRingsLive, baseline.sendRingsLive)
	add("recv_rings", stats.recvRingsLive, baseline.recvRingsLive)
	add("cancels", stats.cancelsLive, baseline.cancelsLive)
	return strings.Join(parts, " ")
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

func readSoakScenarios(t *testing.T) soakScenarios {
	t.Helper()
	all := make(map[string]bool, len(allSoakScenarios))
	selected := make(map[string]bool, len(allSoakScenarios))
	for _, name := range allSoakScenarios {
		all[name] = true
		selected[name] = true
	}
	if only := scenarioSet(os.Getenv("OMQ_GO_SOAK_SCENARIOS")); len(only) > 0 {
		validateSoakScenarioSet(t, only, all)
		selected = only
	}
	skip := scenarioSet(os.Getenv("OMQ_GO_SOAK_SKIP_SCENARIOS"))
	validateSoakScenarioSet(t, skip, all)
	for name := range skip {
		delete(selected, name)
	}
	if len(selected) == 0 {
		t.Fatal("OMQ_GO_SOAK_SCENARIOS selected no soak scenarios")
	}
	return soakScenarios{selected: selected}
}

func scenarioSet(raw string) map[string]bool {
	set := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		set[name] = true
	}
	return set
}

func validateSoakScenarioSet(t *testing.T, set map[string]bool, all map[string]bool) {
	t.Helper()
	for name := range set {
		if !all[name] {
			t.Fatalf("unknown soak scenario %q", name)
		}
	}
}

func (s soakScenarios) enabled(name string) bool {
	return s.selected[name]
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
	fdGrowth               uint64
	finalFDGrowth          uint64
	heapSlopeKiBPerSec     float64
	rssSlopeKiBPerSec      float64
	fdSlopePerSec          float64
	rssTailGrowthPercent   float64
	heapSlopeMinGrowth     uint64
	rssSlopeMinGrowth      uint64
	fdSlopeMinGrowth       uint64
	heapResidualFloorBytes uint64
	rssTailGrowthMinBytes  uint64
}

type soakResources struct {
	heapBytes              uint64
	heapInuseBytes         uint64
	heapIdleBytes          uint64
	heapReleasedBytes      uint64
	heapSysBytes           uint64
	stackInuseBytes        uint64
	sysBytes               uint64
	rssBytes               uint64
	vmRSSBytes             uint64
	vmDataBytes            uint64
	smapsRSSBytes          uint64
	smapsAnonymousBytes    uint64
	smapsPrivateDirtyBytes uint64
	fdCount                uint64
	goroutines             uint64
	cgoCalls               uint64
	threads                uint64
	native                 nativeStats
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
	return soakResourceLimits{
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
		rssTailGrowthPercent: float64Config(
			[]string{"OMQ_GO_SOAK_RSS_TAIL_GROWTH_PERCENT"}, 25,
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
		heapResidualFloorBytes: mibConfig(
			[]string{"OMQ_GO_SOAK_HEAP_RESIDUAL_FLOOR_MB"}, 8,
		),
		rssTailGrowthMinBytes: mibConfig(
			[]string{"OMQ_GO_SOAK_RSS_TAIL_GROWTH_MIN_MB"}, 128,
		),
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

func (r *soakResourceTracker) sample() soakResources {
	current := readSoakResources()
	r.appendSample(time.Now(), current)
	return current
}

func (r *soakResourceTracker) assertLive(t *testing.T, elapsed time.Duration, current soakResources) {
	t.Helper()
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
}

func (r *soakResourceTracker) assertFinal(t *testing.T, elapsed time.Duration) {
	t.Helper()
	current := r.sample()
	t.Logf(
		"[go-soak] final resources heap=%dMB rss=%dMB fds=%d",
		current.heapBytes/1_048_576,
		current.rssBytes/1_048_576,
		current.fdCount,
	)
	logSoakResourceDetails(t, current)
	logSoakNativeStats(t, current.native)
	r.logSlope(t, "heap", r.heap, 1024, "KiB")
	r.logSlope(t, "RSS", r.rss, 1024, "KiB")
	r.logSlope(t, "FD", r.fds, 1, "FDs")
	r.logRSSPeak(t)
	r.assertLive(t, elapsed, current)
	r.assertHeapResidual(t, current)
	r.assertNativeResidual(t, current)
	r.assertRSSResidualStable(t, current)
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

func (r *soakResourceTracker) logRSSPeak(t *testing.T) {
	t.Helper()
	peak := maxSampleValue(r.rss, 0)
	if peak == 0 {
		return
	}
	t.Logf("[go-soak] RSS peak %.1f MiB (informational)", float64(peak)/1_048_576)
}

func logSoakResourceDetails(t *testing.T, current soakResources) {
	t.Helper()
	t.Logf(
		"[go-soak-resources] heap_alloc=%dMB heap_inuse=%dMB heap_idle=%dMB heap_released=%dMB heap_sys=%dMB stack_inuse=%dMB sys=%dMB vmrss=%dMB vmdata=%dMB smaps_rss=%dMB anon=%dMB private_dirty=%dMB",
		current.heapBytes/1_048_576,
		current.heapInuseBytes/1_048_576,
		current.heapIdleBytes/1_048_576,
		current.heapReleasedBytes/1_048_576,
		current.heapSysBytes/1_048_576,
		current.stackInuseBytes/1_048_576,
		current.sysBytes/1_048_576,
		current.vmRSSBytes/1_048_576,
		current.vmDataBytes/1_048_576,
		current.smapsRSSBytes/1_048_576,
		current.smapsAnonymousBytes/1_048_576,
		current.smapsPrivateDirtyBytes/1_048_576,
	)
}

func logSoakNativeStats(t *testing.T, stats nativeStats) {
	t.Helper()
	t.Logf(
		"[go-soak-native] ctx=%d/%d/%d sockets=%d/%d/%d monitors=%d/%d/%d send_rings=%d/%d/%d recv_rings=%d/%d/%d cancels=%d/%d/%d",
		stats.contextsCreated,
		stats.contextsFreed,
		stats.contextsLive,
		stats.socketsCreated,
		stats.socketsFreed,
		stats.socketsLive,
		stats.monitorsCreated,
		stats.monitorsFreed,
		stats.monitorsLive,
		stats.sendRingsCreated,
		stats.sendRingsFreed,
		stats.sendRingsLive,
		stats.recvRingsCreated,
		stats.recvRingsFreed,
		stats.recvRingsLive,
		stats.cancelsCreated,
		stats.cancelsFreed,
		stats.cancelsLive,
	)
}

func logSoakLifecycle(t *testing.T, counters *soakCounters) {
	t.Helper()
	t.Logf(
		"[go-soak-lifecycle] tcp=%s curve=%s compression=%s inproc=%s poller=%s pubsub=%s context-churn=%s",
		counters.lifecycleString("tcp"),
		counters.lifecycleString("curve"),
		counters.lifecycleString("compression"),
		counters.lifecycleString("inproc"),
		counters.lifecycleString("poller"),
		counters.lifecycleString("pubsub"),
		counters.lifecycleString("context-churn"),
	)
	t.Logf("[go-soak-lifecycle] protocol-mix=%s", counters.lifecycleString("protocol-mix"))
}

func (r *soakResourceTracker) assertHeapResidual(t *testing.T, current soakResources) {
	t.Helper()
	peak := maxSampleValue(r.heap, r.baseline.heapBytes)
	threshold := max(peak/20, r.limits.heapResidualFloorBytes)
	growth := saturatingSub(current.heapBytes, r.baseline.heapBytes)
	t.Logf(
		"[go-soak] heap residual %.1f KiB (baseline %.1f MiB, current %.1f MiB, threshold %.1f MiB)",
		float64(growth)/1024,
		float64(r.baseline.heapBytes)/1_048_576,
		float64(current.heapBytes)/1_048_576,
		float64(threshold)/1_048_576,
	)
	if growth > threshold {
		t.Fatalf(
			"heap leak detected: residual %.1f KiB exceeds threshold %.1f MiB",
			float64(growth)/1024,
			float64(threshold)/1_048_576,
		)
	}
}

func (r *soakResourceTracker) assertNativeResidual(t *testing.T, current soakResources) {
	t.Helper()
	if leak := current.native.liveGrowthSince(r.baseline.native); leak != "" {
		t.Fatalf("native handle leak detected: %s", leak)
	}
}

func (r *soakResourceTracker) assertRSSResidualStable(t *testing.T, current soakResources) {
	t.Helper()
	baseline, tailMax, ok := tailGrowthWindow(r.rss)
	if !ok || baseline == 0 {
		return
	}
	tailGrowth := saturatingSub(tailMax, baseline)
	tailGrowthPercent := percentGrowth(tailGrowth, baseline)
	finalGrowth := saturatingSub(current.rssBytes, baseline)
	finalGrowthPercent := percentGrowth(finalGrowth, baseline)
	t.Logf(
		"[go-soak] RSS tail baseline %.1f MiB tail max %.1f MiB growth %.1f%% final growth %.1f%%",
		float64(baseline)/1_048_576,
		float64(tailMax)/1_048_576,
		tailGrowthPercent,
		finalGrowthPercent,
	)
	if !rssResidualLeak(
		baseline,
		tailMax,
		current.rssBytes,
		r.limits.rssTailGrowthPercent,
		r.limits.rssTailGrowthMinBytes,
	) {
		return
	}
	t.Fatalf(
		"RSS leak detected: tail grew %.1f%% / %.1f MiB and final RSS retained %.1f%% / %.1f MiB from post-warmup baseline",
		tailGrowthPercent,
		float64(tailGrowth)/1_048_576,
		finalGrowthPercent,
		float64(finalGrowth)/1_048_576,
	)
}

func readSoakResources() soakResources {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	rss := readRSSBytes()
	if rss == 0 {
		rss = stats.Sys
	}
	status := readProcStatus()
	smaps := readProcSmapsRollup()
	return soakResources{
		heapBytes:              stats.Alloc,
		heapInuseBytes:         stats.HeapInuse,
		heapIdleBytes:          stats.HeapIdle,
		heapReleasedBytes:      stats.HeapReleased,
		heapSysBytes:           stats.HeapSys,
		stackInuseBytes:        stats.StackInuse,
		sysBytes:               stats.Sys,
		rssBytes:               rss,
		vmRSSBytes:             status.vmRSSBytes,
		vmDataBytes:            status.vmDataBytes,
		smapsRSSBytes:          smaps.rssBytes,
		smapsAnonymousBytes:    smaps.anonymousBytes,
		smapsPrivateDirtyBytes: smaps.privateDirtyBytes,
		fdCount:                readFDCount(),
		goroutines:             uint64(runtime.NumGoroutine()),
		cgoCalls:               uint64(runtime.NumCgoCall()),
		threads:                status.threads,
		native:                 nativeStatsNative(),
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

func maxSampleValue(samples []soakResourceSample, fallback uint64) uint64 {
	maxValue := fallback
	for _, sample := range samples {
		maxValue = max(maxValue, sample.value)
	}
	return maxValue
}

func tailGrowthWindow(samples []soakResourceSample) (uint64, uint64, bool) {
	if len(samples) < 10 {
		return 0, 0, false
	}
	warmup := len(samples) / 5
	postWarmup := samples[warmup:]
	if len(postWarmup) < 5 {
		return 0, 0, false
	}
	baselineEnd := max(1, len(postWarmup)/10)
	var baselineSum uint64
	for _, sample := range postWarmup[:baselineEnd] {
		baselineSum += sample.value
	}
	baseline := baselineSum / uint64(baselineEnd)
	tailStart := len(postWarmup) * 4 / 5
	_, tailMax := sampleRange(postWarmup[tailStart:])
	return baseline, tailMax, true
}

func rssResidualLeak(
	baseline uint64,
	tailMax uint64,
	final uint64,
	percentLimit float64,
	minGrowthBytes uint64,
) bool {
	if baseline == 0 {
		return false
	}
	tailGrowth := saturatingSub(tailMax, baseline)
	finalGrowth := saturatingSub(final, baseline)
	if tailGrowth < minGrowthBytes || finalGrowth < minGrowthBytes {
		return false
	}
	return percentGrowth(tailGrowth, baseline) > percentLimit &&
		percentGrowth(finalGrowth, baseline) > percentLimit
}

func percentGrowth(growth uint64, baseline uint64) float64 {
	if baseline == 0 {
		return 0
	}
	return float64(growth) / float64(baseline) * 100
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

type procStatusResources struct {
	vmRSSBytes  uint64
	vmDataBytes uint64
	threads     uint64
}

type procSmapsResources struct {
	rssBytes          uint64
	anonymousBytes    uint64
	privateDirtyBytes uint64
}

func readProcStatus() procStatusResources {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return procStatusResources{}
	}
	return parseProcStatus(string(data))
}

func parseProcStatus(data string) procStatusResources {
	var resources procStatusResources
	for _, line := range strings.Split(data, "\n") {
		switch {
		case strings.HasPrefix(line, "VmRSS:"):
			resources.vmRSSBytes = parseProcKBLine(line)
		case strings.HasPrefix(line, "VmData:"):
			resources.vmDataBytes = parseProcKBLine(line)
		case strings.HasPrefix(line, "Threads:"):
			resources.threads = parseProcUintLine(line)
		}
	}
	return resources
}

func readProcSmapsRollup() procSmapsResources {
	data, err := os.ReadFile("/proc/self/smaps_rollup")
	if err != nil {
		return procSmapsResources{}
	}
	return parseProcSmapsRollup(string(data))
}

func parseProcSmapsRollup(data string) procSmapsResources {
	var resources procSmapsResources
	for _, line := range strings.Split(data, "\n") {
		switch {
		case strings.HasPrefix(line, "Rss:"):
			resources.rssBytes = parseProcKBLine(line)
		case strings.HasPrefix(line, "Anonymous:"):
			resources.anonymousBytes = parseProcKBLine(line)
		case strings.HasPrefix(line, "Private_Dirty:"):
			resources.privateDirtyBytes = parseProcKBLine(line)
		}
	}
	return resources
}

func parseProcKBLine(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	value, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return value * 1024
}

func parseProcUintLine(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	value, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func readFDCount() uint64 {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return uint64(len(entries))
}

func soakNewSocket(
	ctx *Context,
	counters *soakCounters,
	scenario string,
	socketType SocketType,
	opts ...SocketOption,
) (*Socket, error) {
	socket, err := ctx.Socket(socketType, opts...)
	if err != nil {
		return nil, err
	}
	counters.scenarioSocketCreated(scenario)
	return socket, nil
}

func closeSoakSocket(socket *Socket) {
	if socket != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), soakCloseTimeout)
		defer cancel()
		_ = socket.Close(closeCtx)
	}
}

func closeSoakScenarioSocket(socket *Socket, counters *soakCounters, scenario string) {
	if socket == nil {
		return
	}
	state := socket.stateOrNil()
	wasClosed := state == nil || state.closed.Load()
	closeCtx, cancel := context.WithTimeout(context.Background(), soakCloseTimeout)
	defer cancel()
	_ = socket.Close(closeCtx)
	if !wasClosed {
		counters.scenarioSocketClosed(scenario)
	}
}

func closeSoakContext(ctx *Context) error {
	closeCtx, cancel := context.WithTimeout(context.Background(), soakCloseTimeout)
	defer cancel()
	return ctx.CloseContext(closeCtx)
}

func soakStopError(err error) bool {
	return errors.Is(err, ErrCanceled) ||
		errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
