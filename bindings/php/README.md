# OMQ for PHP

The PECL `zmq` extension (`php-zmq`) runs unchanged on `omq-libzmq`. Point the
dynamic loader at `libomq_zmq` under the name `libzmq.so.5` and the existing
`ZMQContext`, `ZMQSocket`, `ZMQPoll`, and `ZMQDevice` code uses OMQ.

<p align="center">
  <img src="https://raw.githubusercontent.com/paddor/omq.rs/main/bindings/php/doc/charts/bindings.svg" alt="ext-zmq on OMQ vs libzmq: TCP throughput and latency" width="850">
</p>

Same PHP code and extension build; only the loaded library differs.

## Use

Install the extension from your distro (`php-zmq` on Debian/Ubuntu,
`php-pecl-zmq` from Remi), then stage OMQ:

```sh
omq-libzmq/scripts/stage-compat.sh --release /opt/omq
LD_LIBRARY_PATH=/opt/omq/lib php app.php
```

For PHP-FPM, set the variable in the service environment, for example a
systemd drop-in with `Environment=LD_LIBRARY_PATH=/opt/omq/lib`. Leave the
system `libzmq.so.5` in place for other programs.

`ZMQ::LIBZMQ_VERSION` reports the libzmq ABI version that OMQ implements.

## What you get

- OMQ transports through plain endpoint strings: `lz4+tcp://`,
  `zstd+tcp://`, `ws://`, plus `tcp://`, `ipc://`, and `inproc://`.
- Persistent contexts and sockets across FPM requests, `ZMQPoll`,
  `ZMQDevice`, socket monitors, CURVE, and `SOCKOPT_FD`/`SOCKOPT_EVENTS` for
  ReactPHP and Revolt loops.

Limits come from the extension: it exposes the classic socket types and
options only, so CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, CHANNEL, PEER,
and OMQ-specific options are out of reach. Compatibility gaps of the C ABI
itself are in [`omq-libzmq/doc/compatibility.md`](../../omq-libzmq/doc/compatibility.md).

## Development

```sh
scripts/test-php.sh                       # drop-in tests against libomq_zmq
ruby bindings/php/scripts/update_perf.rb  # benchmark and chart
```

Both read `OMQ_PHP` for a PHP command with the extension loaded, for example
`OMQ_PHP="php -d extension=zmq.so"`. Benchmark rows go to
`~/.cache/omq.php/bindings.jsonl`.
