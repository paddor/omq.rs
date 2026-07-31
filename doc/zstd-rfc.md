# ZMTP-Zstd: Zstandard-Compressed TCP Transport for ZMTP

| Field    | Value                                              |
|----------|----------------------------------------------------|
| Status   | Draft                                              |
| Editor   | Patrik Wenger                                      |
| Scheme   | `zstd+tcp://`                                      |
| Requires | [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/) |


## 1. Abstract

This specification defines `zstd+tcp://`, a TCP transport for ZMTP 3.1
that applies per-part Zstandard compression after the ZMTP handshake.
Both peers use the `zstd+tcp://` scheme in their endpoint URIs. The ZMTP
greeting and handshake proceed over raw TCP exactly as they would over
`tcp://`. After the handshake completes, every message part on the wire
is individually encoded with a 4-byte sentinel dispatch that
distinguishes uncompressed plaintext, Zstandard-compressed frames, and
dictionary shipments. No ZMTP properties, command frames, or
negotiation are involved. Compression is an intrinsic property of the
transport, like encryption is an intrinsic property of TLS.


## 2. Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).


## 3. Motivation

Zstandard at low compression levels encodes in single-digit microseconds
per kilobyte, decompresses faster still, and on dictionary-trained
workloads compresses small frames to a fraction of their size. For most
ZMTP deployments compression can be treated as almost free CPU-wise,
while recovering large fractions of the wire budget.

Network-bound or bandwidth-constrained deployments (publish/subscribe
fan-out, cross-region replication, IoT telemetry) trade a small amount
of CPU for a large reduction in wire time. Zstandard's dictionary mode
is a good fit for the small-message profile typical of ZMQ workloads.

ZMTP applications today either accept the wire cost or layer ad-hoc,
per-payload compression into the application format. The latter requires
both sides to opt in and bakes compression into the payload rather than
the transport. `zstd+tcp://` replaces it with a transport-level
mechanism that any ZMTP application benefits from without changes to the
payload.

### 3.1 Why a transport scheme

Compression could live at three layers. Each has a fatal flaw except the
transport layer.

**Socket-level wrapper** (too high). A wrapper above routing knows
nothing about transports. It compresses local connections (pure
overhead) and cannot act on new connections naturally. Dictionary
shipping requires per-connection state, but a wrapper only sees messages
after routing has dispatched them. Reconnect handling requires hooking
into connection lifecycle events that are awkward from outside.

**ZMTP connection layer** (too low). Embedding compression into each
ZMTP connection means fan-out patterns compress the same message N times
(once per subscriber connection). The connection layer has no
socket-wide view, so there is no way to share compression work across
connections.

**Transport layer** (right). `zstd+tcp://` makes transport selection
explicit in the endpoint URI. Only TCP connections get compressed. Local
transports are unaffected even on the same socket. Dictionary lifetime
matches connection lifetime naturally (new connection = new wrapper =
re-ship dictionary). No negotiation is needed; both peers use
`zstd+tcp://`. The codec is socket-wide (shared across connections), so
fan-out patterns compress once and reuse the result.

### 3.2 Why not negotiate

ZMTP 3.1 already supports unknown READY properties. An unaware peer
silently ignores them. A negotiation-based design could fall back to
plaintext when the peer does not understand compression. But this
introduces complexity (profile matching, asymmetric per-direction state,
passive senders) for a marginal benefit: in practice, compression is a
deployment decision, not a runtime discovery. Both peers are configured
to use `zstd+tcp://` or they are not. The transport scheme approach
eliminates the entire negotiation surface and its edge cases.

### 3.3 Why Zstandard

Zstandard at low levels matches LZ4 on encode latency, beats it on
decompression speed and ratio at every realistic ZMQ payload size, and
has a first-class dictionary story. The decompression advantage is
particularly important for fan-out patterns (PUB/SUB, RADIO/DISH): the
publisher pays one compress, every subscriber pays decompress, so
per-subscriber CPU dominates the total budget.


## 4. Goals and Non-goals

### 4.1 Goals

- Transparent to application code: send/receive operations see plaintext.
- Per-part sender decision: opt out for short or incompressible parts.
- Works for legacy multipart socket types (PUSH/PULL, PUB/SUB, ...) and
  draft single-frame types alike.
- Small-message-friendly via an optional shared dictionary, either
  supplied out of band or automatically trained from early traffic.
- No ZMTP-level negotiation, no new READY properties, no new command
  frames.

### 4.2 Non-goals

- New ZMTP mechanism, new socket type, new greeting, new frame flag bit.
- Compression of the ZMTP greeting or command frames (READY, SUBSCRIBE,
  PING, PONG, ...).
- Application to non-TCP transports (`inproc://` is zero-copy;
  compression is pure overhead; `ipc://` rarely benefits).
- Replacing or weakening CurveZMQ or any other security mechanism.
  See Sec. 9.
- Streaming / context-takeover compression. Each part is decodable in
  isolation with no dependency on a previous part's LZ77 history.


## 5. Terminology

| Term                | Meaning                                                              |
|---------------------|----------------------------------------------------------------------|
| Part                | One ZMTP message frame body. A multipart message has multiple parts. |
| Sentinel            | The first 4 bytes of a post-handshake part on the wire (Sec. 6.1).   |
| Uncompressed part   | A wire part whose sentinel is `00 00 00 00`.                         |
| Compressed part     | A wire part whose first 4 bytes are the Zstandard magic `28 B5 2F FD`. |
| Dictionary part     | A wire part whose first 4 bytes are `37 A4 30 EC` (Sec. 7).         |
| Dictionary message  | A single-part ZMTP message consisting of exactly one dictionary part. |


## 6. Part Encoding

After the ZMTP handshake completes, every message part on the wire is
individually encoded. The ZMTP MORE flag is carried on the wire frame
header as normal. Multipart messages are encoded part by part; each
part is independent.

### 6.1 Sentinel dispatch

The first 4 bytes of each wire part determine how it is decoded.

| Sentinel (hex)   | Meaning                              |
|------------------|--------------------------------------|
| `00 00 00 00`    | Uncompressed plaintext (Sec. 6.3)    |
| `28 B5 2F FD`    | Zstandard compressed frame (Sec. 6.4)|
| `37 A4 30 EC`    | Dictionary shipment (Sec. 7)         |

All other 4-byte values are reserved. A receiver that encounters an
unknown sentinel MUST close the connection.

### 6.2 Compression level

This specification suggests **level 1** as an implementation default.
This keeps encode throughput high while improving wire ratio over
negative fast levels. This is not a wire requirement. Implementations
may choose any default level that fits their deployment goals, and
bandwidth-constrained deployments may choose higher levels such as 3.

The compression level is a sender choice and is not communicated on the
wire. The receiver decodes any valid Zstandard frame regardless of the
level used to encode it. Implementations SHOULD expose the level as a
configurable parameter.

### 6.3 Uncompressed sentinel `00 00 00 00`

```
+------------------+-------------------+
| 00 00 00 00      | plaintext payload |
| (4 bytes)        | (N bytes)         |
+------------------+-------------------+
```

The sender uses this sentinel when it decides not to compress the part.
The 4-byte overhead is the price of per-part selective compression
without an extra flag bit in the ZMTP frame header.

Four zero bytes cannot collide with a valid Zstandard frame magic or the
dictionary sentinel, so no ambiguity arises.

### 6.4 Compressed Zstandard frame

```
+------------------+
| Zstandard frame  |
| (M bytes)        |
+------------------+
```

The wire part IS the Zstandard frame. Its first 4 bytes are the
standard Zstandard frame magic `28 B5 2F FD`. No additional framing is
added.

The sender MUST configure the encoder to write the `Frame_Content_Size`
field in the Zstandard frame header (RFC 8878 Sec. 3.1.1.1.2). This
field is required for the receiver's budget enforcement (Sec. 6.6).

### 6.5 Sender rules

For each outgoing message part, the sender proceeds as follows:

1. Compute `min_size`:
   - If a dictionary is currently installed: **64 bytes**.
   - Otherwise: **512 bytes**.

   These thresholds reflect empirical measurement: without a dictionary,
   Zstandard cannot usefully compress typical payloads below ~512 bytes;
   with a dictionary, even 64-byte payloads compress to ~20 bytes.
   Implementations MAY tune these thresholds.

2. If `plaintext_size < min_size`, prepend `00 00 00 00` and emit.

3. Otherwise, run the Zstandard encoder. The encoder MUST write the
   `Frame_Content_Size` field. If the compressed output's size is
   >= `plaintext_size - 4` (net saving <= 0 after accounting for the
   4-byte sentinel of the uncompressed alternative), prepend
   `00 00 00 00` and emit the plaintext instead. Otherwise emit the
   Zstandard frame as-is.

4. If the plaintext's first 4 bytes happen to be `28 B5 2F FD` or
   `37 A4 30 EC` and the sender chooses not to compress, the sender
   MUST still prepend `00 00 00 00` to avoid sentinel ambiguity.
   Step 2 and step 3's fallback path already guarantee this.

### 6.6 Receiver rules

For each incoming wire part, the receiver proceeds as follows:

1. Read the first 4 bytes as the sentinel. If the part is shorter than
   4 bytes, close the connection.

2. Sentinel `00 00 00 00`: the remaining `N - 4` bytes are plaintext.
   Return them.

3. Sentinel `28 B5 2F FD`: the entire wire part is a Zstandard frame.
   - Read the `Frame_Content_Size` field from the Zstandard header. If
     the field is absent, close the connection.
   - If the connection enforces a maximum message size, add this part's
     declared content size to the running decompressed total for the
     current multipart message (parts chained by the ZMTP MORE flag).
     If the running total would exceed the maximum, close the connection
     without invoking the decoder.
   - Invoke the decoder in a bounded mode that aborts if it would write
     more bytes than `Frame_Content_Size` declared. On such an abort,
     close the connection.
   - Return the decompressed plaintext.

4. Sentinel `37 A4 30 EC`: dictionary shipment. See Sec. 7.

5. Any other sentinel: close the connection.

The maximum message size always refers to the **decompressed** plaintext
summed across all parts of a multipart message. A multipart message
whose total wire length is small but whose total decompressed size
exceeds the limit MUST be rejected before decoder invocation.


## 7. Dictionary Shipment

### 7.1 Dictionary message format

A dictionary is shipped as a **single-part ZMTP message** (no MORE flag)
whose body is exactly a Zstandard dictionary blob in ZDICT format. ZDICT
blobs begin with the dictionary magic `37 A4 30 EC`; this transport uses
those first 4 bytes as the dictionary sentinel:

```
+------------------+------------------------+
| 37 A4 30 EC      | rest of ZDICT blob     |
| (4 bytes)        | (D - 4 bytes)          |
+------------------+------------------------+
```

The sentinel `37 A4 30 EC` is the standard Zstandard dictionary magic
(`DICT_MAGIC`, little-endian). It is intentionally reused as this
transport's dictionary discriminator because a ZDICT blob already carries
it and it does not collide with the Zstandard frame magic or the
uncompressed sentinel.

The complete `D` bytes, including the leading `37 A4 30 EC` magic, are
the raw dictionary as it should be passed to the Zstandard decoder's
dictionary-load operation.

### 7.2 Constraints

- A dictionary message MUST be a single-part ZMTP message (MORE flag
  not set on the frame header). A dictionary sentinel in a multipart
  message's non-final or non-only part is a protocol error.

- A dictionary message MUST NOT exceed **8 KiB** total. Since the
  message body is exactly the ZDICT blob, this is also the maximum
  dictionary size. A receiver that receives a dictionary message larger
  than 8 KiB MUST close the connection.

- A sender MUST send at most **one** dictionary message per direction
  per connection. A receiver that receives a second dictionary message
  on the same connection MUST close the connection.

- A dictionary message MUST be sent BEFORE any compressed part that
  references the dictionary. In practice this means the sender ships
  the dictionary before (or immediately after training triggers during)
  the first compressed write that would benefit from it.

### 7.3 Receiver handling

When the receiver encounters a dictionary part:

1. Validate the constraints in Sec. 7.2.
2. Install the complete message body as the decompression dictionary for
   this connection. The leading `37 A4 30 EC` bytes are part of the ZDICT
   blob and MUST NOT be stripped.
4. Discard the message. It is not delivered to the application.

If all parts of a ZMTP message are dictionary parts (which is always
the case, since dictionary messages are single-part), the receiver
loops to receive the next message.

### 7.4 Dictionary scope

The dictionary a sender ships applies to a single direction of a single
connection. Each peer may independently ship its own dictionary for its
own send direction. The common deployment is one-directional: a
publisher ships its dictionary; subscribers decode with it and send
nothing (or uncompressed traffic) back.

The sender's dictionary is typically socket-wide: trained once from
early traffic across all connections and reused. But this is an
implementation choice. The wire protocol carries no dictionary identity
or scope metadata.

An implementation MAY pool training samples and share the resulting
auto-trained dictionary across all `zstd+tcp://` connections of a
single socket. This is beneficial when a socket binds or connects
multiple `zstd+tcp://` endpoints: samples from one endpoint accelerate
training for all of them, and newly opened connections benefit from a
dictionary trained by their predecessors. Connections that were
configured with an explicit out-of-band dictionary MUST NOT participate
in shared training; they use their own dictionary independently.

### 7.5 Automatic dictionary training

A sender MAY train a dictionary automatically from early traffic.
The training algorithm, trigger condition, and parameters are
implementation choices. The following wire-level constraints MUST be
respected:

- A trained dictionary MUST NOT exceed the maximum dictionary size
  (Sec. 7.2: 8 KiB).
- A sender MUST ship at most one dictionary per direction per
  connection (Sec. 7.2).
- If training fails (the sample set was too small or too uniform),
  the sender MUST stay in no-dictionary mode for the rest of the
  socket's lifetime. It MUST NOT retry training.

The following parameters are RECOMMENDED for implementations that
support auto-training:

| Parameter              | Recommended value                        |
|------------------------|------------------------------------------|
| Dictionary capacity    | 2 KiB                                    |
| Training trigger       | 1000 samples OR 100 KiB (first reached)  |
| Max sample length      | 2048 bytes                               |
| Training algorithm     | FastCOVER                                |

Whether auto-training is enabled by default is an implementation
choice. Applications MAY disable it or supply an out-of-band
dictionary instead.

### 7.6 Dictionary ID

Auto-trained dictionaries SHOULD be patched with a random dictionary ID
in the Zstandard user range (32768 to 2^31 - 1) to avoid collisions
with Zstandard's built-in dictionary IDs. Out-of-band dictionaries
retain whatever dictionary ID they were created with.


## 8. ZMTP Interaction

### 8.1 Greeting and handshake

The ZMTP greeting and security mechanism handshake proceed over raw TCP
exactly as specified by RFC 37. `zstd+tcp://` does not modify the
greeting, mechanism, READY properties, or any command frames. The
compression layer activates only after the handshake is complete and the
connection is ready for message traffic.

### 8.2 Command frames

ZMTP command frames (READY, SUBSCRIBE, CANCEL, JOIN, LEAVE, PING,
PONG) are never compressed. They are sent and received as standard ZMTP
command frames. Only message frames (the COMMAND bit not set in the
frame header) are subject to sentinel-dispatched encoding.

### 8.3 Socket type compatibility

`zstd+tcp://` is compatible with all ZMTP socket types. The socket type
negotiation in the READY handshake is unaffected.

### 8.4 Peer requirement

Both peers of a connection MUST use `zstd+tcp://`. There is no
fallback to plaintext TCP and no negotiation. A `zstd+tcp://` peer
connecting to a plain `tcp://` peer (or vice versa) will see garbled
data or sentinel errors and the connection will fail.


## 9. Security Considerations

### 9.1 Compression combined with encryption (CRIME / BREACH)

Combining length-revealing compression with a secure channel that
carries attacker-influenced plaintext enables CRIME- and BREACH-style
side-channel attacks. An attacker who can inject chosen bytes into the
plaintext and observe the ciphertext length can extract secrets byte
by byte.

Implementations SHOULD refuse to layer `zstd+tcp://` inside an
encrypted tunnel when the plaintext contains attacker-controlled
content. Deployments that accept this risk MUST do so with explicit
opt-in.

### 9.2 Length side-channel

Compression makes the wire length of a part depend on its content. An
on-path observer can learn something about the plaintext from the
compressed length alone. Deployments that care about traffic analysis
MUST NOT rely on `zstd+tcp://` to hide payload shape.

### 9.3 Dictionary contents

When auto-training is enabled, the receiver loads dictionary bytes
chosen by the peer. The Zstandard reference dictionary loader is
hardened against malformed inputs, but implementations MUST enforce the
8 KiB cap on dictionary messages (Sec. 7.2) and SHOULD NOT cache
received dictionaries across connections.

### 9.4 Decompression bombs

A small compressed frame can decompress to many megabytes of plaintext.
The receiver rules in Sec. 6.6 mitigate this:

1. Every compressed part MUST carry `Frame_Content_Size`. The receiver
   checks the declared total against the maximum message size before
   invoking the decoder, so a bomb is rejected on its header alone.
2. The decoder is invoked in bounded mode. It aborts if it would write
   more bytes than declared. A peer that lies in the header cannot
   expand a part past its declared size.

Implementations SHOULD set a conservative maximum message size on
`zstd+tcp://` connections even if they would otherwise leave it
unbounded.


## 10. Constants

| Constant                | Value                                         |
|-------------------------|-----------------------------------------------|
| Uncompressed sentinel   | `00 00 00 00`                                 |
| Zstd frame sentinel     | `28 B5 2F FD` (Zstandard frame magic)         |
| Dictionary sentinel     | `37 A4 30 EC`                                 |
| Suggested default level | 1                                             |
| Min compress, no dict   | 512 bytes                                     |
| Min compress, with dict | 64 bytes                                      |
| Max dictionary size     | 8 KiB                                         |
| Train max samples       | 1000                                          |
| Train max bytes         | 100 KiB                                       |
| Train max sample length | 2048 bytes                                    |
| Dictionary capacity     | 2 KiB                                         |


## 11. References

- [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/)
- [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119)
- [RFC 8878: Zstandard Compression Data Format](https://datatracker.ietf.org/doc/html/rfc8878)
- [Zstandard dictionary builder](https://github.com/facebook/zstd/blob/dev/lib/dictBuilder/zdict.h)
- [CRIME attack](https://en.wikipedia.org/wiki/CRIME)
- [BREACH attack](https://en.wikipedia.org/wiki/BREACH)
- [`lz4+tcp://` RFC](lz4-rfc.md)
