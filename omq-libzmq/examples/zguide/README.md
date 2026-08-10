# omq-libzmq ZGuide Examples

C examples that exercise the `omq-libzmq` libzmq-compatible ABI.

Build everything:

```sh
omq-libzmq/examples/zguide/build.sh
```

Run one suite:

```sh
omq-libzmq/examples/zguide/01_req_rep/run.sh
```

Run every suite:

```sh
omq-libzmq/examples/zguide/run_all.sh
```

These mirror the runnable Rust ZGuide suites in `examples/zguide/`. They use
`zmq.h` and link against `libomq_zmq`.

Original ZGuide C examples that use only the stable libzmq C API should build
against this header/library. Examples using CZMQ (`zsock_t`, `zmsg_t`,
`zpoller_t`) need CZMQ itself. C++ examples need cppzmq and only work for the
API surface `omq-libzmq` implements.
