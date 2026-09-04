# libzmq v4.3.5 vs omq.rs: Error Handling Gap Analysis

Compares error/edge-case handling in libzmq v4.3.5 against what omq.rs
implements today. See `errors.md` for the full libzmq catalog and
`semantics.md` for no-peer send, linger, and HWM behavior.

## Legend

- **=** parity (same or equivalent handling)
- **~** partial / different-but-adequate approach
- **X** not handled in omq.rs
- **N/A** not applicable (architecture differs)

---

## 1. Low-Level I/O

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| EAGAIN/EWOULDBLOCK/EINTR | Explicit normalize + return 0 | Implicit via async runtime | **~** runtime handles it |
| ECONNRESET/EPIPE/ETIMEDOUT | Return -1 (peer failure) | Fatal, driver exits -> reconnect | **=** |
| Partial writes | Advance pointer, retry on POLLOUT | `put_back_unwritten` re-queues | **=** |
| Clean close (read=0) | Set `errno=EPIPE`, return -1 | `UnexpectedEof`, driver exits | **=** |
| WSAENOBUFS on send (Windows) | Return 0 (transient, KB201213) | Not handled | **X** |
| FreeBSD close() ECONNRESET | Silently accepted in destructor | Not handled | **X** |
| iOS EBADF excluded from assert | Platform-specific carve-out | Not handled | **X** |
| Write error asymmetry | Write fail != teardown; wait for read-side error | Write fail = driver exit | **~** different design; omq.rs reconnects instead |

omq.rs delegates low-level errno wrangling to tokio. The Windows
`WSAENOBUFS` and FreeBSD close quirks are platform bugs that could bite on
those platforms. The write-error asymmetry is a deliberate design
difference: libzmq keeps the engine alive to drain inbound; omq.rs tears
down and reconnects.

---

## 2. TCP Connect

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Async connect + SO_ERROR | Manual non-blocking + getsockopt | Runtime-native `TcpStream::connect` | **=** equivalent |
| EINTR during connect | Normalize to EINPROGRESS | Runtime handles | **~** |
| Connect timeout | Separate timer (`connect_timeout`) | Handshake timeout (30s default) covers it | **~** combined timeout |
| ECONNREFUSED + stop flag | `RECONNECT_STOP_CONN_REFUSED` -> no retry | No equivalent flag; always retries | **X** |
| Solaris ENOPROTOOPT compat | Treat as errno=0 | Not handled | **X** niche |

---

## 3. TCP Accept

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Accept error classification | Per-platform acceptable errno lists; abort on unknown | Any accept error backs off 50 ms and retries | **~** less granular; no event |
| EMFILE/ENFILE | In acceptable list (but TODO: no special handling) | Backoff and retry through generic accept error path | **=** |
| Post-accept tune failure | Emit EVENT_ACCEPT_FAILED, skip engine | Accepted connection is dropped; accept loop retries | **~** no event |
| accept4(SOCK_CLOEXEC) | Used where available | Not explicitly set | **X** |
| SO_EXCLUSIVEADDRUSE (Windows) | Yes (not SO_REUSEADDR) | Not set, OS default | **X** |
| SO_REUSEADDR (POSIX) | Explicit | Set before bind via socket2 | **=** |
| TCP accept filters (allowlist) | Address-based accept filtering | Not implemented | **X** |

---

## 4. Socket Options

| Option | libzmq | omq.rs | Gap? |
|--------|--------|--------|------|
| TCP_NODELAY | Yes | Yes | **=** |
| SO_SNDBUF / SO_RCVBUF | Yes, via `assert_success_or_recoverable` | Yes, via socket2 | **=** |
| Keepalive (time/interval/retries) | Yes, platform-specific | Yes, via socket2 `TcpKeepalive` | **=** |
| TCP_USER_TIMEOUT (Linux) | Yes | No | **X** |
| TCP_MAXRT (Windows) | Yes | No | **X** |
| SO_NOSIGPIPE (macOS/BSD) | Yes | No (relies on runtime) | **~** tokio uses MSG_NOSIGNAL on send |
| SO_BUSY_POLL (Linux) | Yes | No | **X** |
| SIO_LOOPBACK_FAST_PATH (Win) | Yes | No | **X** |
| SOCK_CLOEXEC / FD_CLOEXEC | Yes | No | **X** |
| SO_BINDTODEVICE (Linux) | Yes | No | **X** |
| TOS / IP priority | Yes | No | **X** |

---

## 5. IPC Transport

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Path length validation | `>= sizeof(sun_path)` -> ENAMETOOLONG | Delegated to OS bind/connect | **~** |
| Abstract sockets (Linux) | `@` prefix -> `\0` in sun_path | Yes, via `from_abstract_name()` | **=** |
| Peer credential filtering | SO_PEERCRED, UID/GID/PID checks | Not implemented | **X** |
| Socket file cleanup on close | unlink + rmdir, EVENT_CLOSE_FAILED on error | Drop handler removes file | **=** |
| Wildcard IPC address | Temp dir + generated path | Not implemented | **X** |

---

## 6. Reconnection

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Exponential backoff | Double each attempt, cap at max | Exponential with +/-10% jitter | **=** better (jitter) |
| Fixed interval + jitter (no max) | base + random % base | `Fixed(d)` with +/-10% jitter | **=** |
| Overflow guard | INT_MAX cap | Duration arithmetic; no overflow risk | **=** |
| RECONNECT_STOP_CONN_REFUSED | Stop on ECONNREFUSED | Not implemented | **X** |
| RECONNECT_STOP_HANDSHAKE_FAILED | Reclassify as protocol_error | Protocol errors don't retry; timeout/IO do | **~** similar effect |
| RECONNECT_STOP_AFTER_DISCONNECT | Stop after zmq_disconnect() | CancellationToken on disconnect | **=** equivalent |
| Resubscription on reconnect | Hiccup pipe -> SUB/XSUB/DISH resend subs | Subscriptions re-sent on new connection | **=** |

---

## 7. Engine I/O Loop

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Write error != teardown | Disable POLLOUT, wait for read error | Driver exits, reconnect | **~** different design |
| Backpressure (input_stopped) | reset_pollin, restart_input | Bounded channel back-pressure | **=** equivalent |
| Speculative write | out_event() called immediately on restart_output | Async runtime schedules writes | **~** |
| Recv throttling | Tick counter limits command processing | No equivalent; async runtime schedules | **~** |

---

## 8. Pipe Layer / HWM

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| HWM enforcement | per-pipe complete-message credits | Per-pipe/ring message caps; transmit slots separate, so effective capacity can exceed one `send_hwm` | **~** |
| LWM re-activation at half | (hwm+1)/2 triggers activate_write | Channel internal (flume/async_channel) | **~** |
| Round-robin HWM exceeded | Blocks / EAGAIN to user | Block / DropNewest / DropOldest | **=** superset |
| PUB/XPUB mute | Drop on HWM (`XPUB_NODROP` excepted) | Drop on HWM (`xpub_nodrop` excepted) | **=** |
| Pipe termination FSM | 6-state machine with delimiter protocol | Channel close + CancellationToken | **~** simpler |
| Multi-part rollback on pipe death | unwrite() loop, _dropping mode in lb | Whole messages are queued; no mid-message rollback | **N/A** |
| Conflate mode | HWM=-1 (unlimited) | capacity=1, DropOldest | **=** equivalent semantics |
| Lock-free queue | ypipe (SPSC, CAS flush) | yring (SPSC, batched flush/prefetch) | **=** equivalent design |

---

## 9. Routing Strategies

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| LB round-robin (PUSH/DEALER) | Index-based, skip full pipes; no ready bound pipe mutes sends; connect pipes can queue before READY | Active per-peer pipes plus connect-side pre-ready pipes; bound no-pipe sends mute | **~** |
| FQ round-robin (PULL/ROUTER recv) | Index-based, assert on mid-msg pipe death | Per-peer drivers feed bounded recv queue | **~** |
| PUB fan-out: slow sub blocks all | No; slow sub mutes and drops | Per-sub independent queues; slow sub drops | **=** |
| PUB fan-out: ref counting | Manual rm_refs on failure | Bytes Arc clone | **=** equivalent |
| ROUTER mandatory | EHOSTUNREACH | Error::Unroutable | **=** |
| ROUTER handover | Reassign old conn integral ID | Always-on; evict old peer, `Disconnected(Handover)` | **=** better (no opt-in needed) |
| ROUTER anonymous pipes | Separate set until identified | Not mentioned | **~** |
| lb _dropping mode (mid-msg pipe death) | Silently consume remaining frames | Different architecture (whole-message queuing) | **N/A** |

---

## 10. Security Mechanisms

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| NULL | READY exchange + optional ZAP filtering | Same | **=** |
| PLAIN | Username/password in HELLO, policy through ZAP | Same | **=** |
| CURVE (RFC 26) | Full, with optional ZAP policy | Same; native API also supports inline authenticators | **=** |
| ZAP protocol | Context-local inproc REP/ROUTER handler | Same application-facing protocol | **=** |
| Socket type compat check | 18-type matrix | 18-type matrix | **=** |
| Mechanism mismatch | EVENT + protocol_error | Error::HandshakeFailed | **=** |
| CURVE nonce replay check | nonce <= previous -> INVALID_SEQUENCE | Nonce exhaustion check; replay? | **~** verify |
| ZAP "300" silent disconnect | No ERROR sent to peer | Same | **=** |

---

## 11. Protocol Error Detection

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| Reserved flag bits | Abort on set | Error::Protocol | **=** |
| COMMAND + MORE | Abort | Error::Protocol | **=** |
| Malformed commands | Per-command size checks | Per-command size + content checks | **=** |
| PING missing TTL | Error | Error::Protocol | **=** |
| PING context >16 bytes | Error | Error::Protocol | **=** |
| ERROR reason not UTF-8 | Not checked (binary) | Error::Protocol | **=** stricter |
| READY property truncated | Error | Error::Protocol | **=** |
| Data frame during handshake | Not explicitly checked | Error::HandshakeFailed | **=** |
| Max message size | Not built-in (app-level) | Error::MessageTooLarge | **=** better |

---

## 12. Monitor Events

| libzmq Event | omq.rs Equivalent | Gap? |
|-------------|-------------------|------|
| CONNECTED | `Connected` | **=** |
| CONNECT_DELAYED | `ConnectDelayed` (with retry_in + attempt) | **=** better |
| CONNECT_RETRIED | `ConnectDelayed` | **=** merged |
| LISTENING | `Listening` | **=** |
| BIND_FAILED | Not emitted (bind is sync, returns Result) | **~** |
| ACCEPTED | `Accepted` | **=** |
| ACCEPT_FAILED | Not emitted | **X** |
| CLOSED | `Closed` | **=** |
| CLOSE_FAILED | Not emitted | **X** |
| DISCONNECTED | `Disconnected` (with reason enum) | **=** better |
| MONITOR_STOPPED | `Closed` | **=** |
| HANDSHAKE_FAILED_NO_DETAIL | `HandshakeFailed` (with reason string) | **=** better |
| HANDSHAKE_SUCCEEDED | `HandshakeSucceeded` (with full PeerInfo) | **=** better |
| HANDSHAKE_FAILED_PROTOCOL | `HandshakeFailed` (unified) | **~** no separate code |
| HANDSHAKE_FAILED_AUTH | `HandshakeFailed` | **~** no separate code |
| none | `PeerCommand` (ERROR + unknown cmds) | **=** omq-only |

---

## 13. Heartbeat

| Area | libzmq | omq.rs | Gap? |
|------|--------|--------|------|
| PING/PONG | ZMTP 3.1, auto-answer | ZMTP 3.1, auto-answer | **=** |
| heartbeat_ivl | Timer to send PING | heartbeat_interval | **=** |
| heartbeat_ttl | Advertised to peer | heartbeat_ttl (advisory) | **=** |
| heartbeat_timeout | No PONG -> timeout_error | No bytes received -> Timeout | **~** byte-level vs PONG |
| Handshake timeout | Separate timer | handshake_timeout (30s default) | **=** |

---

## Gaps Worth Considering

### Platform-specific

- **WSAENOBUFS transient handling (Windows):** libzmq returns 0 (retry)
  instead of -1. Workaround for Windows kernel bug KB201213. omq.rs would
  treat this as a fatal write error on Windows.
- **SO_EXCLUSIVEADDRUSE (Windows):** libzmq sets this on listeners instead
  of SO_REUSEADDR. Prevents port hijacking on Windows.
- **TCP_USER_TIMEOUT (Linux) / TCP_MAXRT (Windows):** Caps kernel-level
  retransmit time. Without it, a dead peer can hold a connection for
  minutes before the kernel gives up.
- **SOCK_CLOEXEC / FD_CLOEXEC:** Prevents fd leak to child processes on
  fork+exec. Not critical for typical Rust programs (no fork), but matters
  if the process spawns children.
- **SO_BUSY_POLL, SIO_LOOPBACK_FAST_PATH, TOS/priority:** Performance
  knobs. Low priority.

### Resilience

- **Accept failure observability:** Per-accept failures back off and
  retry, but no `ACCEPT_FAILED` monitor event exposes them to users.
- **RECONNECT_STOP_CONN_REFUSED:** No way to tell omq.rs "stop retrying
  if the peer actively refuses." Always retries until socket close.

### Features

- **IPC peer credential filtering (SO_PEERCRED):** Cannot restrict IPC
  connections by UID/GID/PID.
- **IPC wildcard address:** Cannot auto-generate a unique IPC path.
- **TCP accept filter socket option:** `ZMQ_TCP_ACCEPT_FILTER` is unsupported;
  use a NULL ZAP domain for address-based admission policy.
- **ROUTER handover:** Implemented. Always-on (libzmq gates behind
  `ZMQ_ROUTER_HANDOVER`). Old connection is evicted with
  `DisconnectReason::Handover`. Applies to ROUTER and SERVER.
