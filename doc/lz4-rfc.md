# ZMTP-LZ4: LZ4-Compressed TCP Transport for ZMTP

| Field    | Value                                                                 |
|----------|-----------------------------------------------------------------------|
| Status   | Draft                                                                 |
| Editor   | Patrik Wenger                                                         |
| Scheme   | `lz4+tcp://`                                                          |
| Requires | [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/), LZ4 block format |


## 1. Abstract

This document specifies `lz4+tcp://`, an LZ4-compressed variant of the
ZMTP 3.1 transport. After a plain-TCP handshake, every post-handshake
ZMTP message part is framed with a 4-byte sentinel and compressed with
the LZ4 block algorithm (no entropy stage). An optional dictionary
(user-supplied or automatically trained) is shipped in-band exactly once
per direction per connection.


## 2. Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this
document are to be interpreted as described in
[RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119).


## 3. Motivation

### 3.1 Why a transport scheme

Compression as a transport scheme (`lz4+tcp://` vs. `tcp://`) rather
than a socket option: the decision is in the endpoint URI, so the
two peers agree on compression implicitly by choosing the same
scheme. No handshake negotiation, no mixed-mode ambiguity on the
wire, no socket option that can silently fall back to plain on
misconfiguration. An `lz4+tcp://` peer will not successfully talk
to a `tcp://` peer. The handshake succeeds, but the first
post-handshake part fails sentinel dispatch and the connection
closes.

### 3.2 Why not negotiate compression

A single scheme committed by both sides is simpler than any
in-protocol negotiation. Negotiation introduces per-connection
state, a failure mode for inconsistent configuration, and a
timing-based side channel for probing whether a peer speaks the
compressed variant. The scheme-is-the-contract model avoids all of
that.

### 3.3 Why LZ4 block format, not frame

Frame format (magic `04 22 4D 18`, with header + end-mark + optional
checksum) is meant for standalone interoperable files. Over a
ZMTP-framed transport, the per-part length is already carried by
ZMTP's own frame header, and the interoperability case (piping one
part through the `lz4` CLI) is not a goal. Block format saves 11-19
bytes of envelope per part, avoids a redundant content checksum
(TCP already has one; ZMTP over CURVE has Poly1305), and keeps the
encoder/decoder allocation footprint small.


## 4. Goals and Non-goals

### 4.1 Goals

- Transparent compression: any ZMTP message part that works over
  plain `tcp://` works over `lz4+tcp://`, with no size limit.
- Per-part LZ4 block-format compression, independently decodable.
- Optional dictionary (user-supplied or auto-trained), shipped in-band
  exactly once per direction per connection, capped at 8 KiB.
- Pass-through for parts where compression does not save >= 8 bytes.
- Bounded decompression: every decoder invocation is capped by a
  caller-supplied or socket-level `max_message_size`.
- Sentinel-dispatched receiver: four legal sentinels, all others
  close the connection.
- No negotiation state, no mixed-mode per-connection.

### 4.2 Non-goals

- LZ4 frame-format interoperability. Wire bytes intentionally do not
  match any standard LZ4 tool; the sentinels (`LZ4B`, `LZ4M`, `LZ4D`)
  are ASCII-encoded and not in any defined LZ4 magic-number range.
- Custom dictionary training algorithms. The built-in COVER trainer
  is sufficient; pluggable trainers are not a goal.
- Streaming / context-takeover compression. Each part is
  independently decodable from its own LZ4B envelope.
- Multiple dict rotation on one connection. A connection gets at most
  one dict per direction; to rotate, close and reopen. The same dict
  bytes MAY be reused across many connections.
- `lz4+ipc://`, `lz4+inproc://`. No meaningful bandwidth win over
  plain `ipc://` or `inproc://`; inproc is zero-copy.


## 5. Terminology

| Term     | Meaning                                                                      |
|----------|------------------------------------------------------------------------------|
| Sentinel | First 4 bytes of each wire part, selecting which of three formats follows.   |
| Dict     | User-supplied or trained byte string, 1-8192 bytes. Shipment and receive-side install are tracked per direction per connection; the same dict bytes MAY be shared by many connections. |
| Envelope | The sentinel + any fixed header bytes that precede the payload or ciphertext. |
| Budget   | The socket's `max_message_size`, enforced by the receiver as a cap on the total decompressed size summed across all parts of a single ZMTP message (MORE-flag-chained). |


## 6. Part Encoding

### 6.1 Sentinel dispatch

Every wire part begins with one of four 4-byte sentinels:

| Sentinel (hex) | ASCII  | Meaning                     |
|----------------|--------|-----------------------------|
| `00 00 00 00`  | (none) | Uncompressed plaintext      |
| `4C 5A 34 42`  | `LZ4B` | LZ4-compressed single block |
| `4C 5A 34 4D`  | `LZ4M` | LZ4-compressed multi-block  |
| `4C 5A 34 44`  | `LZ4D` | Dictionary shipment         |

Any other leading 4 bytes MUST cause the receiver to close the
connection.

### 6.2 Uncompressed part (`00 00 00 00`)

```
+------------------+-------------------+
| 00 00 00 00      | plaintext payload |
| (4 bytes)        | (N bytes)         |
+------------------+-------------------+
```

Length is implicit from the ZMTP frame size: `N = frame_size - 4`.

### 6.3 LZ4-compressed single-block part (`LZ4B`)

```
+--------------+--------------------------+------------------+
| 4C 5A 34 42  | decompressed_size u64 LE | LZ4 block bytes  |
| (4 bytes)    | (8 bytes)                | (M bytes)        |
+--------------+--------------------------+------------------+
```

- `decompressed_size` is the exact plaintext length the decoder will
  produce. Required because LZ4 block format carries no length
  prefix; the receiver pre-sizes its output buffer to this value
  and refuses to write past it.
- `M = frame_size - 12`. Bytes are the raw output of
  `LZ4_compress_fast_extState` (or equivalent): no magic, no
  descriptor, no end-mark, no checksum.
- `decompressed_size` MUST NOT exceed `LZ4M_BLOCK_SIZE`
  (1,073,741,824). Parts larger than this MUST use the LZ4M
  multi-block encoding (Sec. 6.4).

### 6.4 LZ4-compressed multi-block part (`LZ4M`)

```
+--------------+--------------------------+-----+-----+-----+-----+
| 4C 5A 34 4D  | decompressed_size u64 LE | BL0 | B0  | BL1 | B1  | ...
| (4 bytes)    | (8 bytes)                | (4) | (?) | (4) | (?) |
+--------------+--------------------------+-----+-----+-----+-----+
```

Each block is a `(u32 LE compressed_block_len, LZ4 block bytes)` pair:

- `compressed_block_len` is the byte length of the immediately
  following LZ4-compressed block.
- The decompressed size of block `i` is
  `min(LZ4M_BLOCK_SIZE, decompressed_size - i * LZ4M_BLOCK_SIZE)`.
  The last block may be smaller than `LZ4M_BLOCK_SIZE`.
- Each block is independently decodable. When a dictionary is
  installed, each block is decompressed against the installed dict
  independently (no cross-block context).
- Blocks are read sequentially until `decompressed_size` bytes have
  been recovered. The sum of all blocks' decompressed sizes MUST
  equal `decompressed_size`; a mismatch closes the connection.
- `LZ4M_BLOCK_SIZE` = 1,073,741,824 (1 GiB, `0x40000000`). This
  value is a protocol constant chosen to stay well within the LZ4
  block API's `i32` parameter limit.

### 6.5 Sender rules

1. Choose a per-part `min_size`. The RECOMMENDED conservative value is
   128 bytes if a dictionary is installed on this connection's send
   side, else 512 bytes. Implementations MAY use a lower value when
   their compressor, dictionary trainer, and workload measurements show
   a net benefit.
2. If `plaintext.size < min_size`: emit
   `00 00 00 00 | plaintext`.
3. If `plaintext.size > LZ4M_BLOCK_SIZE`: use multi-block encoding
   (Sec. 6.6).
4. Otherwise compress the plaintext against the installed dict (or
   no dict). If
   `compressed.size + 12 >= plaintext.size + 4`
   (net saving <= 0 after accounting for the 8-byte envelope
   overhead), fall back to uncompressed.
5. Otherwise emit
   `LZ4B | decompressed_size u64 LE | compressed`.

If the plaintext's leading 4 bytes happen to match a reserved
sentinel and the sender elected passthrough, the `00 00 00 00`
prefix already disambiguates at the receiver.

### 6.6 Multi-block sender rules

When `plaintext.size > LZ4M_BLOCK_SIZE`:

1. Split the plaintext into consecutive chunks of `LZ4M_BLOCK_SIZE`
   bytes; the last chunk is the remainder.
2. Compress each chunk independently against the installed dict (or
   no dict).
3. Emit `LZ4M | decompressed_size u64 LE`, followed by
   `u32 LE compressed_block_len | compressed_block` for each chunk.

No plaintext fallback is applied per-block. The multi-block path
always compresses. (Parts this large benefit from compression even
at a modest ratio; the passthrough threshold is designed for small
messages where the 12-byte envelope overhead dominates.)

### 6.7 Receiver rules

1. Read 4-byte sentinel. If the part is shorter than 4 bytes, close
   the connection.
2. `00 00 00 00`: check remaining byte count against the budget
   (Sec. 8); return the remaining `N - 4` bytes as plaintext.
3. `LZ4B`:
   - If the part is shorter than 12 bytes, close the connection
     (no room for the size field).
   - Read 8-byte `decompressed_size`.
   - If `decompressed_size > LZ4M_BLOCK_SIZE`, close the connection
     (single-block parts MUST NOT exceed the block size limit).
   - If this part's declared size plus the running budget spent so
     far on prior parts of this message exceeds
     `max_message_size`, close BEFORE invoking the decoder.
   - Invoke `LZ4_decompress_safe` (or the block-format equivalent)
     with output buffer pre-sized to `decompressed_size`.
   - On any failure (truncated input, decoder overrun, malformed
     block, declared-size lie caught by the decoder), close the
     connection.
   - Return plaintext.
4. `LZ4M`:
   - If the part is shorter than 12 bytes, close the connection
     (no room for the size field).
   - Read 8-byte `decompressed_size`.
   - If this part's declared size plus the running budget spent so
     far on prior parts of this message exceeds
     `max_message_size`, close BEFORE invoking the decoder.
   - Allocate an output buffer of `decompressed_size` bytes.
   - Read blocks sequentially: for each block, read 4-byte
     `u32 LE compressed_block_len`, then `compressed_block_len`
     bytes of compressed data. Compute the block's decompressed
     size as `min(LZ4M_BLOCK_SIZE, remaining)`.
   - Invoke `LZ4_decompress_safe` (or `_usingDict` if a dict is
     installed) for each block, writing into the output buffer at
     the current offset.
   - If any block fails, or the total decompressed output does not
     equal `decompressed_size`, or there are leftover bytes after
     the last block, close the connection.
   - Return plaintext.
5. `LZ4D`: see Sec. 7.
6. Any other sentinel: close the connection.


## 7. Dictionary Shipment

### 7.1 Dictionary message format

```
+--------------+---------------------------+
| 4C 5A 34 44  | dictionary bytes          |
| (4 bytes)    | (D bytes, 1 <= D <= 8192) |
+--------------+---------------------------+
```

### 7.2 Constraints

- A dictionary shipment MUST be a single-part ZMTP message. The
  MORE flag MUST be clear on the sole frame. A receiver MUST close
  the connection if an LZ4D-prefixed frame appears with MORE set or
  in a multi-part message.
- `D` MUST satisfy `1 <= D <= 8192`. `D = 0` or `D > 8192`: close
  the connection.
- A shipment MUST be sent BEFORE any LZ4B part that relies on the
  dict.
- A shipment MUST be sent AT MOST ONCE per direction per connection.
  A second LZ4D shipment on the same direction: close the connection.
- Dictionary shipments are consumed by the transport layer; they
  MUST NOT be delivered to the application.

### 7.3 Sender dictionary scope

The dictionary is a byte string, not a streaming compression context.
Implementations MAY choose its ownership scope. It can be configured per
socket, trained per socket, trained per fan-out lane, or trained per
connection.

The wire requirement is narrower: before sending any LZ4B part that was
compressed with dict `D` on a connection, the sender MUST have shipped
`LZ4D | D` on that same direction of that same connection, and MUST NOT
ship a second dictionary on that direction. If many connections use the
same dict bytes, a sender may encode a payload once with that dict and
reuse the resulting LZ4B bytes for every connection that has already
received that dict.

No-dictionary LZ4B/LZ4M is a valid subset of this transport.

### 7.4 Receiver handling

The receiver validates the shipment against Sec. 7.2 and installs the
dict on the current connection's receive side. Subsequent LZ4B
parts on that direction MUST be decoded against the installed dict.

### 7.5 Size budget accounting

Dictionary shipments do NOT count against the receiver's
`max_message_size` budget. They are transport overhead, not
messages.

### 7.6 Dict mismatch detection (not implemented)

LZ4 block format has no `Dict_ID` field and no built-in checksum.
This specification does NOT define a dictionary-id carried in the
shipment or per-message envelope. A dict mismatch between peers
produces garbage plaintext at the receiver, not a transport-level
error.

Applications that need mismatch detection MUST validate at the
application layer (schema validation, payload checksum, etc.).
This reflects LZ4's minimal-overhead design goal.

### 7.7 Automatic dictionary training

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
| Training trigger       | 100 messages                             |
| Max sample length      | 2 * dict_capacity                        |
| Training algorithm     | COVER (d-mer frequency selection)        |

Whether auto-training is enabled by default is an implementation
choice. Applications MAY disable it or supply an out-of-band
dictionary instead.


## 8. Receiver Size Budget

The receiver MUST bound decompression by the socket's
`max_message_size`. Implementations MUST apply the bound as a cap
on the total decompressed size of all parts in a single ZMTP
message (parts chained by the MORE flag), starting at
`max_message_size` and shrinking by each part's plaintext size as
the message is decoded.

The bound is enforced BEFORE the decoder is invoked whenever
possible:

- LZ4B parts: use the declared `decompressed_size` from the
  envelope.
- UNCOMPRESSED parts: use the wire part length.

If the budget is exhausted, the connection closes.

If `max_message_size` is not set (unlimited), no bound is enforced.


## 9. ZMTP Interaction

### 9.1 Greeting and handshake

The ZMTP 3.1 greeting, READY exchange, and all command frames
(SUBSCRIBE, PING, etc.) run over raw TCP, uncompressed. Only
post-handshake message parts are LZ4-framed. This avoids a
compression-timing side channel during handshake (when per-peer
identity and subscription data could leak) and keeps the plain-TCP
contract of the handshake unchanged.

### 9.2 Socket type compatibility

All ZMTP 3.1 socket type combinations that work over plain `tcp://`
work over `lz4+tcp://`, including draft types (CLIENT/SERVER,
RADIO/DISH, etc.). The transport is orthogonal to routing.

### 9.3 Peer requirement

Both peers MUST use the `lz4+tcp://` scheme. A `tcp://` peer cannot
talk to an `lz4+tcp://` peer. The handshake succeeds, but the
first post-handshake part fails sentinel dispatch on the
`lz4+tcp://` side and closes.


## 10. Security Considerations

### 10.1 CRIME / BREACH-style compression oracles

Compression combined with encryption where an attacker controls
part of the plaintext and observes the ciphertext size creates a
byte-by-byte oracle for the non-attacker-controlled portion. This
class of attack applies to any compression-then-encryption
transport, including `lz4+tcp://` used with CURVE.

Applications that (a) run CURVE, and (b) concatenate attacker-
controlled and secret data into a single compressed part SHOULD
NOT use `lz4+tcp://`. Use `tcp://` with CURVE instead, or separate
the attacker-controlled and secret data into different parts (the
per-part boundary prevents cross-part dictionary sharing).

### 10.2 Length side-channel

Compressed part sizes leak structural information about the
plaintext (entropy, repeating patterns) to a passive wire observer.
Applications that require length-independent confidentiality MUST
pad at the application layer before compression, or use plain
`tcp://`.

### 10.3 Dictionary contents

A dictionary shipped on the wire is visible to any intermediary
with access to the ciphertext stream (the shipment itself is the
raw bytes, uncompressed). Do NOT use dictionaries that contain
secrets. A dictionary is an optimization hint, not a secret.

### 10.4 Decompression bombs

A malicious peer could craft an LZ4B or LZ4M part with a declared
`decompressed_size` close to `usize::MAX`, causing the receiver to
allocate a huge output buffer. The per-part size-budget check (Sec. 8)
defends against this: the cap is applied to `decompressed_size`
BEFORE the buffer is allocated. Operators SHOULD set a sensible
`max_message_size` on every `lz4+tcp://` socket; the default
unlimited policy is appropriate only when the application fully
trusts its peers.

For LZ4B, `decompressed_size` is additionally capped at
`LZ4M_BLOCK_SIZE` (1 GiB) by the receiver (Sec. 6.7 rule 3). Parts
larger than this MUST use LZ4M, where the total allocation is
still governed by the declared `decompressed_size` and the budget.

LZ4 block format cannot exhibit the pathological expansion ratios
that general-purpose compressors can. The ratio
is bounded by the spec at roughly 255x, still large enough that
an unbounded budget is not safe on untrusted peers.


## 11. Constants

| Constant                      | Value                                    |
|-------------------------------|------------------------------------------|
| Scheme                        | `lz4+tcp`                                |
| Uncompressed sentinel         | `00 00 00 00`                            |
| Single-block sentinel         | `4C 5A 34 42` (`LZ4B`)                   |
| Multi-block sentinel          | `4C 5A 34 4D` (`LZ4M`)                   |
| Dictionary sentinel           | `4C 5A 34 44` (`LZ4D`)                   |
| LZ4M block size               | 1,073,741,824 (1 GiB, `0x40000000`)      |
| Max dictionary size           | 8192 bytes                               |
| Min compress size, no dict    | 512 bytes (RECOMMENDED)                  |
| Min compress size, with dict  | 128 bytes (conservative RECOMMENDED)     |
| LZ4 acceleration              | 1 (default)                              |
| LZ4B envelope size            | 12 bytes (4 sentinel + 8 size)           |
| LZ4M envelope size            | 12 + 4*N bytes (N = number of blocks)    |
| UNCOMPRESSED envelope size    | 4 bytes (sentinel)                       |
| LZ4D envelope size            | 4 bytes (sentinel)                       |


## 12. References

- [LZ4 Block Format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md)
- [RFC 37/ZMTP 3.1](https://rfc.zeromq.org/spec/37/)
- [RFC 2119](https://datatracker.ietf.org/doc/html/rfc2119)
