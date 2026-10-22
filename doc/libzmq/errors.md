# libzmq v4.3.5 Error Handling Catalog

How libzmq detects and handles edge cases, network errors, and protocol
violations. Source: `zeromq/libzmq` tag `v4.3.5`.

---

## 1. Low-Level I/O (`tcp.cpp`, `ip.cpp`)

### tcp_write (non-blocking send)

| Platform | Condition | Action |
|----------|-----------|--------|
| POSIX | `EAGAIN` / `EWOULDBLOCK` / `EINTR` | Return 0 (no bytes written, not fatal) |
| POSIX | `EPIPE`, `ECONNRESET`, `EHOSTUNREACH`, `ENETUNREACH`, `ETIMEDOUT` | Return -1 (peer failure) |
| POSIX | `EBADF`, `EFAULT`, `ENOMEM`, `ENOTSOCK`, etc. | `errno_assert` abort (bug) |
| iOS | `EBADF` | Excluded from assert (allowed) |
| Windows | `WSAEWOULDBLOCK` | Return 0 |
| Windows | `WSAENOBUFS` | Return 0 (**not** -1) — workaround for Windows bug KB201213 |
| Windows | `WSAENETDOWN`, `WSAECONNRESET`, `WSAETIMEDOUT`, etc. | Return -1 |

### tcp_read (non-blocking recv)

| Platform | Condition | Action |
|----------|-----------|--------|
| POSIX | `EWOULDBLOCK` / `EINTR` | Normalize to `EAGAIN` |
| POSIX | `ECONNRESET`, `ETIMEDOUT`, etc. | Return -1 with errno |
| POSIX | `EBADF`, `EFAULT`, `ENOMEM`, `ENOTSOCK` | Assert abort (bug) |
| iOS | `EBADF` | Excluded from assert |
| Windows | `WSAEWOULDBLOCK` | Set `errno = EAGAIN`, return -1 |
| Windows | `WSAECONNRESET`, `WSAENOBUFS`, etc. | Translate to errno, return -1 |

### stream_engine_base read wrapper

Peer TCP close (read returns 0) → set `errno = EPIPE`, return -1. This is
how clean disconnects are detected.

### Partial writes

`tcp_write` returns actual bytes written. `out_event` advances the output
pointer and retries on next `POLLOUT`. No `EINTR` retry loop — `EINTR`
treated same as `EAGAIN`.

### Socket setup helpers

All `setsockopt` calls (`TCP_NODELAY`, `SO_SNDBUF`, `SO_RCVBUF`,
keepalives, `TCP_USER_TIMEOUT`, `SO_BUSY_POLL`, TOS, priority) go through
`assert_success_or_recoverable`.

**Recoverable errnos** (not aborted):
- POSIX: `ECONNREFUSED`, `ECONNRESET`, `ECONNABORTED`, `EINTR`,
  `ETIMEDOUT`, `EHOSTUNREACH`, `ENETUNREACH`, `ENETDOWN`, `ENETRESET`,
  `EINVAL`
- Windows adds: `WSAEACCES`, `WSAEADDRINUSE`

Anything else → assert abort.

---

## 2. TCP Connect (`tcp_connecter.cpp`, `stream_connecter_base.cpp`)

### Async connect flow

1. `open()` calls `connect()` on non-blocking socket.
2. Returns 0 → immediate success → `out_event()`.
3. Returns -1 / `EINPROGRESS` → register for `POLLOUT`, emit
   `EVENT_CONNECT_DELAYED`, start connect timeout timer.
4. Other error → close fd, `add_reconnect_timer()`.

### EINTR during connect

`connect()` returning `EINTR` is normalized to `EINPROGRESS` (Linux can
return `EINTR` mid-connect).

### Connect completion (SO_ERROR check)

`getsockopt(SO_ERROR)` retrieves the async result:
- Error 0 → success, hand fd to engine.
- `ECONNREFUSED` + `ZMQ_RECONNECT_STOP_CONN_REFUSED` → `conn_failed` to
  session, terminate, **no reconnect**.
- Other errors → close, reconnect timer.
- Solaris: if `getsockopt` itself fails with `ENOPROTOOPT`, treats as
  errno=0 (compat hack).
- Asserts that error is NOT `EBADF`, `ENOPROTOOPT`, `ENOTSOCK`, `ENOBUFS`
  (those are bugs). iOS excludes `EBADF`.

### Connect timeout

Separate timer (`options.connect_timeout`). If it fires before `POLLOUT`
resolves → close fd, reconnect timer. IPC connecter does **not** have a
connect timeout (TODO in source).

---

## 3. TCP Accept (`tcp_listener.cpp`)

### accept() failure handling

Uses `accept4(SOCK_CLOEXEC)` where available, else `accept()`.

**Acceptable errors (return `retired_fd`, caller ignores):**

| Platform | Accepted errnos |
|----------|----------------|
| POSIX | `EAGAIN`, `EWOULDBLOCK`, `EINTR`, `ECONNABORTED`, `EPROTO`, `ENOBUFS`, `ENOMEM`, `EMFILE`, `ENFILE` |
| Android | Above + `EINVAL` |
| Windows | `WSAEWOULDBLOCK`, `WSAECONNRESET`, `WSAEMFILE`, `WSAENOBUFS` |

Anything else → assert abort. TODO: `EMFILE`/`ENFILE` should be handled
specially (resource exhaustion) but aren't.

### Post-accept

- TCP accept filters checked (address allowlist). Mismatch → close fd.
- `set_nosigpipe()` failure → close fd, return `retired_fd`.
- `tune_tcp_socket` / keepalives / maxrt failure → emit
  `EVENT_ACCEPT_FAILED`, don't create engine.

### Listener bind

- Windows uses `SO_EXCLUSIVEADDRUSE` (**not** `SO_REUSEADDR`).
- POSIX uses `SO_REUSEADDR`.
- Supports pre-created fd (`use_fd`) — skips socket creation entirely.

---

## 4. IPC Transport (`ipc_connecter.cpp`, `ipc_listener.cpp`)

### IPC-specific address validation

- Path ≥ `sizeof(sun_path)` → `ENAMETOOLONG`.
- Abstract socket `"@"` alone (no name after `@`) → `EINVAL`.
- Abstract sockets: `@` prefix replaced with `\0` in `sun_path`.
- Non-null-terminated `sun_path` handled (per `unix(7)` NOTES).

### IPC listener cleanup

On close: unlinks socket file and removes temp directory (if wildcard
address was used). `unlink`/`rmdir` failure → `EVENT_CLOSE_FAILED`.

### IPC peer credential filtering (`SO_PEERCRED` / `LOCAL_PEERCRED`)

- No filters → accept all.
- `getsockopt(SO_PEERCRED)` failure → reject.
- BSD: `cr_version != XUCRED_VERSION` → reject.
- Checks peer UID/GID/PID against configured filter sets.
- `getpwuid` failure → reject.

---

## 5. Reconnection & Backoff (`stream_connecter_base.cpp`, `session_base.cpp`)

### Backoff algorithm

Two modes:

**With `reconnect_ivl_max > 0` (exponential, no jitter):**
Doubles `_current_reconnect_ivl` each attempt, capped at
`reconnect_ivl_max`. Overflow guard: if current > `INT_MAX/2`, cap at
`INT_MAX`.

**Without max (base + random jitter, no growth):**
`reconnect_ivl + (random() % reconnect_ivl)`. Base never increases.
Overflow guard clamps to `INT_MAX`.

### Reconnect stop flags (`ZMQ_RECONNECT_STOP`)

| Flag | Value | Effect |
|------|-------|--------|
| `CONN_REFUSED` | 0x1 | Stop on `ECONNREFUSED` |
| `HANDSHAKE_FAILED` | 0x2 | Treat connection/timeout error during handshake as protocol error (no reconnect) |
| `AFTER_DISCONNECT` | 0x4 | Stop after explicit `zmq_disconnect()` |

### Session error routing

```
engine_error(reason)
├── protocol_error      → terminate (no reconnect)
├── connection_error    → reconnect (if active/outgoing session)
│                       → terminate (if passive/incoming session)
└── timeout_error       → reconnect (if active)
                        → terminate (if passive)
```

### Reconnect cleanup

Before reconnecting: rolls back unflushed output, flushes upstream, loops
to discard incomplete inbound messages. For SUB/XSUB/DISH sockets: hiccups
inbound pipe to trigger resubscription.

---

## 6. Engine I/O Loop (`stream_engine_base.cpp`)

### Write error asymmetry

Write failure (`tcp_write` returns -1) does **NOT** terminate the engine.
Only stops polling for output. Engine waits for input-side error to tear
down. Prevents losing in-flight incoming messages.

### Recv loop

1. `read()` returns -1 / `errno != EAGAIN` → `error(connection_error)`.
2. Decode loop: bytes → decoder → `_process_msg()`.
3. `_process_msg` returns -1 / `EAGAIN` → `_input_stopped = true`,
   `reset_pollin()` (backpressure from session).
4. Other decode error → `error(protocol_error)`.

### Restart after backpressure

`restart_input()` retries the pending decoded message. If `_io_error` flag
was set while stopped → `error(connection_error)` (deferred teardown).

### Speculative write

`restart_output()` calls `out_event()` immediately after setting
`POLLOUT` — latency optimization, avoids waiting for next poll cycle.

### Heartbeat / PING-PONG

| Timer | Fires when | Action |
|-------|-----------|--------|
| `heartbeat_ivl` | Time to send PING | Produce PING, trigger `out_event`, re-arm |
| `heartbeat_ttl` | Remote TTL expired without PING | `error(timeout_error)` |
| `heartbeat_timeout` | No PONG received | `error(timeout_error)` |
| `handshake_timer` | Handshake took too long | `error(timeout_error)` |

### Engine error handler

1. ROUTER + disconnect notification → roll back incomplete msg, push
   disconnect notification.
2. During handshake + not protocol_error → `EVENT_HANDSHAKE_FAILED_NO_DETAIL`.
3. If `RECONNECT_STOP_HANDSHAKE_FAILED` set → reclassify as protocol_error.
4. Always emit `EVENT_DISCONNECTED`.
5. Flush session, call `session->engine_error(reason)`.
6. Unplug and `delete this`.

---

## 7. Pipe Layer (`pipe.cpp`, `lb.cpp`, `fq.cpp`, `dist.cpp`)

### Pipe termination state machine (6 states)

```
active ──terminate()──→ term_req_sent1 ──peer_ack──→ cleanup + delete
   │                         │
   │ process_pipe_term()     │ process_pipe_term() (simultaneous)
   │                         ↓
   │ (delay=true)        term_req_sent2 ──peer_ack──→ cleanup + delete
   ↓
waiting_for_delimiter ──delimiter──→ term_ack_sent ──→ peer deletes us
   │
   │ (delay=false)
   ↓
term_ack_sent
```

Duplicate `terminate()` calls ignored if already in progress.

### HWM (high water mark) flow control

- `check_hwm()`: pipe full when `msgs_written - peers_msgs_read >= hwm`.
- LWM = `(hwm + 1) / 2` — reader sends `activate_write` to peer at half
  capacity.
- Conflate mode: HWM = -1 (unlimited).
- Inproc: sums HWMs from both sides (`sndhwm + peer.rcvhwm`), 0 if either
  is 0.

### Load balancer (PUSH/DEALER send)

- No active pipes → `EAGAIN`.
- Write failure mid-multi-part → `_dropping = true`: remaining frames
  silently consumed until final frame. Returns -2 (special code).
- Round-robin: advances after each complete message.

### Fair queue (PULL/ROUTER recv)

- `pipe->read()` failure mid-multi-part → `zmq_assert` abort (protocol
  violation — should never happen with correct codec).
- Round-robin: advances after each complete message.

### Distribution (PUB/XPUB fan-out)

Three-region pipe segmentation: `[0, matching)`, `[0, active)`,
`[0, eligible)`. Write failure during fan-out:
- VSM: iterates without incrementing index.
- Large messages: pre-allocates `matching - 1` refs, calls `rm_refs(failed)`
  on failure to prevent ref leaks.
- A subscriber at HWM is skipped and loses the message. With
  `ZMQ_XPUB_NODROP`, the send fails with EAGAIN instead.

### Multi-part atomicity

All routing strategies (`lb`, `fq`, `dist`) track a `_more` flag. A pipe
dying mid-message triggers rollback (`_out_pipe->unwrite()` loop) or
drop-mode.

### Lock-free queue (ypipe)

Single-writer/single-reader. `flush()` uses CAS on `_c` to make items
visible; returns false if reader asleep (caller must signal wake).
`unwrite()` can retract only unflushed items.

---

## 8. Socket Layer (`socket_base.cpp`)

### Context termination

Nearly every public method checks `_ctx_terminated` → returns -1 / `ETERM`.

### Tag validation

Active socket: `_tag == 0xbaddecaf`. After `close()`: set to `0xdeadbeef`.
Prevents use-after-close.

### send() edge cases

- Invalid message (`msg->check()` fails) → `EFAULT`.
- `xsend()` returns -2 → dead pipe mid-multi-part, remaining frames
  silently dropped in blocking mode.
- Blocking: retries with timeout recalculation.

### recv() throttling

Tick counter (`_ticks`): processes mailbox commands only every
`inbound_poll_rate` calls. Prevents command processing from starving recv.

### ROUTER-specific

- `_mandatory = true`: `EHOSTUNREACH` for missing routing ID,
  `EAGAIN` for full pipe.
- `_mandatory = false` (default): silently drops unroutable messages.
- Duplicate routing IDs: rejected unless `_handover` enabled (old connection
  gets new integral ID and is terminated).
- Anonymous pipes held in separate set until identification completes.

---

## 9. Security Mechanisms

### NULL mechanism

- Both sides exchange READY.
- ERROR command from peer → mechanism status = `error`.
- ZAP reply "300" (temporary failure) → silent disconnect, no ERROR sent
  (per CURVEZMQ RFC).
- ZAP reply "400"/"500" → send ERROR to peer.

### PLAIN mechanism

| Error | Event |
|-------|-------|
| Wrong command for state | `ZMTP_UNEXPECTED_COMMAND` |
| HELLO missing username/password fields | `ZMTP_MALFORMED_COMMAND_HELLO` |
| WELCOME wrong size | `ZMTP_MALFORMED_COMMAND_WELCOME` |
| ERROR command truncated | `ZMTP_MALFORMED_COMMAND_ERROR` |
| Metadata parse failure | `ZMTP_INVALID_METADATA` |

### CURVE mechanism

| Error | Event |
|-------|-------|
| `crypto_box_open` failure | `ZMTP_CRYPTOGRAPHIC` |
| HELLO size != 200 | `ZMTP_MALFORMED_COMMAND_HELLO` |
| INITIATE size < 257 | `ZMTP_MALFORMED_COMMAND_INITIATE` |
| READY size < 30 | `ZMTP_MALFORMED_COMMAND_READY` |
| Cookie decryption failure | `ZMTP_CRYPTOGRAPHIC` |
| Vouch key mismatch | `ZMTP_KEY_EXCHANGE` |
| Message nonce ≤ previous (replay) | `ZMTP_INVALID_SEQUENCE` |
| CURVE version != 1.0 | `ZMTP_MALFORMED_COMMAND_HELLO` |

### ZMTP greeting

- Mechanism name mismatch → `ZMTP_MECHANISM_MISMATCH`.
- ZMTP < 3.0 with ZAP enabled → `protocol_error` (reject).
- Malformed command (size ≤ 1 or name-length exceeds msg) →
  `ZMTP_MALFORMED_COMMAND_UNSPECIFIED`.

### ZAP reply validation

| Check | Error code |
|-------|-----------|
| Frame MORE flags wrong | `ZAP_MALFORMED_REPLY` |
| Address delimiter not empty | `ZAP_UNSPECIFIED` |
| Version != "1.0" | `ZAP_BAD_VERSION` |
| Request ID mismatch | `ZAP_BAD_REQUEST_ID` |
| Status code not 3 chars or not `[2-5]00` | `ZAP_INVALID_STATUS_CODE` |
| Metadata frame parse failure | `ZAP_INVALID_METADATA` |

### Socket type compatibility matrix

Full check during handshake: REQ↔REP/ROUTER, DEALER↔REP/DEALER/ROUTER,
PUSH↔PULL, PUB↔SUB/XSUB, PAIR↔PAIR, SERVER↔CLIENT, RADIO↔DISH,
GATHER↔SCATTER, DGRAM↔DGRAM, PEER↔PEER, CHANNEL↔CHANNEL. Mismatch →
`EINVAL`.

---

## 10. Monitor Events (complete list)

| Event | Value | Trigger |
|-------|-------|---------|
| `CONNECTED` | 0x0001 | Connection established (connect side) |
| `CONNECT_DELAYED` | 0x0002 | Async connect in progress |
| `CONNECT_RETRIED` | 0x0004 | Reconnect timer started |
| `LISTENING` | 0x0008 | Listener bound |
| `BIND_FAILED` | 0x0010 | Bind failed |
| `ACCEPTED` | 0x0020 | Connection accepted |
| `ACCEPT_FAILED` | 0x0040 | Accept failed |
| `CLOSED` | 0x0080 | Socket fd closed |
| `CLOSE_FAILED` | 0x0100 | Close/unlink failed (IPC) |
| `DISCONNECTED` | 0x0200 | Connection dropped |
| `MONITOR_STOPPED` | 0x0400 | Monitor stopped |
| `HANDSHAKE_FAILED_NO_DETAIL` | 0x0800 | Handshake I/O / ZAP connect failure |
| `HANDSHAKE_SUCCEEDED` | 0x1000 | Handshake completed |
| `HANDSHAKE_FAILED_PROTOCOL` | 0x2000 | ZMTP/ZAP protocol violation |
| `HANDSHAKE_FAILED_AUTH` | 0x4000 | ZAP auth denied (300/400/500) |

### Protocol error codes (HANDSHAKE_FAILED_PROTOCOL values)

**ZMTP (0x1000xxxx):**

| Code | Value | Description |
|------|-------|-------------|
| `UNSPECIFIED` | 0x10000000 | Invalid server state |
| `UNEXPECTED_COMMAND` | 0x10000001 | Wrong command for state |
| `INVALID_SEQUENCE` | 0x10000002 | CURVE nonce replay/reorder |
| `KEY_EXCHANGE` | 0x10000003 | CURVE vouch key mismatch |
| `MALFORMED_UNSPECIFIED` | 0x10000011 | Basic command structure invalid |
| `MALFORMED_MESSAGE` | 0x10000012 | CURVE MESSAGE too small |
| `MALFORMED_HELLO` | 0x10000013 | HELLO wrong size/fields |
| `MALFORMED_INITIATE` | 0x10000014 | INITIATE too small |
| `MALFORMED_ERROR` | 0x10000015 | ERROR cmd truncated |
| `MALFORMED_READY` | 0x10000016 | READY too small |
| `MALFORMED_WELCOME` | 0x10000017 | WELCOME wrong size |
| `INVALID_METADATA` | 0x10000018 | Metadata parse fail |
| `CRYPTOGRAPHIC` | 0x11000001 | Any crypto failure |
| `MECHANISM_MISMATCH` | 0x11000002 | Greeting mechanism mismatch |

**ZAP (0x2000xxxx):**

| Code | Value | Description |
|------|-------|-------------|
| `UNSPECIFIED` | 0x20000000 | Delimiter not empty |
| `MALFORMED_REPLY` | 0x20000001 | Frame flags wrong |
| `BAD_REQUEST_ID` | 0x20000002 | Request ID mismatch |
| `BAD_VERSION` | 0x20000003 | Version != "1.0" |
| `INVALID_STATUS_CODE` | 0x20000004 | Bad status format |
| `INVALID_METADATA` | 0x20000005 | Metadata parse fail |

---

## 11. Platform-Specific Differences

| Behavior | Linux | macOS | FreeBSD | Windows | iOS |
|----------|-------|-------|---------|---------|-----|
| Accept | `accept4(SOCK_CLOEXEC)` | `accept()` | `accept4(SOCK_CLOEXEC)` | `accept()` | `accept()` |
| Listener addr reuse | `SO_REUSEADDR` | `SO_REUSEADDR` | `SO_REUSEADDR` | `SO_EXCLUSIVEADDRUSE` | `SO_REUSEADDR` |
| SIGPIPE prevention | `MSG_NOSIGNAL` | `SO_NOSIGPIPE` | `SO_NOSIGPIPE` | N/A | `SO_NOSIGPIPE` |
| Keepalive idle | `TCP_KEEPIDLE` | `TCP_KEEPALIVE` | `TCP_KEEPIDLE` | `SIO_KEEPALIVE_VALS` | `TCP_KEEPALIVE` |
| Max retransmit | `TCP_USER_TIMEOUT` (ms) | — | — | `TCP_MAXRT` (sec) | — |
| Loopback fast path | — | — | — | `SIO_LOOPBACK_FAST_PATH` (Win8+) | — |
| Busy poll | `SO_BUSY_POLL` | — | — | — | — |
| Close `ECONNRESET` | assert | assert | silently accepted | assert | assert |
| Send `WSAENOBUFS` | N/A | N/A | N/A | return 0 (transient) | N/A |
| `EBADF` in asserts | included | included | included | included | excluded |
| IPv6 fallback | `EAFNOSUPPORT` → retry IPv4 | same | same | same | same |

---

## 12. Notable Design Decisions

**Write errors don't kill the engine.** A failed `tcp_write` only disables
`POLLOUT`. The engine stays alive until the read side detects the broken
connection. Prevents losing in-flight incoming messages.

**EINTR is never retried in a loop.** It is normalized to EAGAIN (read) or
EINPROGRESS (connect) and the event loop handles the retry.

**`WSAENOBUFS` on Windows send is transient.** Returns 0 (no bytes written)
instead of -1. Workaround for known Windows kernel bug (KB201213).

**FreeBSD close can ECONNRESET.** Under load, `close()` on FreeBSD returns
`ECONNRESET`. libzmq silently accepts this in the engine destructor.

**FQ assert on mid-message pipe death.** If a pipe dies mid-multi-part
during recv, libzmq aborts (`zmq_assert`). This should never happen with a
correct codec — the pipe termination protocol drains complete messages
before tearing down.

**ZAP "300" = silent disconnect.** Temporary auth failure (status "300")
does not send an ERROR command to the peer. Just drops the connection. Per
CURVEZMQ RFC.

**Recv throttling.** `socket_base_t::recv()` only processes mailbox
commands every `inbound_poll_rate` calls. Prevents command processing from
starving the recv hot path.
