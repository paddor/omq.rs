# OMQ Gleam

Gleam wrapper for OMQ.beam.

This package depends on the Erlang `omq` package, which owns the Rust NIF and
native OMQ runtime. The Gleam module exposes typed handles and `Result`
return values around the Erlang API.

```gleam
import omq_gleam as omq

pub fn example() {
  let assert Ok(ctx) = omq.context()
  let assert Ok(pull) = omq.socket(ctx, omq.pull())
  let assert Ok(push) = omq.socket(ctx, omq.push())
  let assert Ok(endpoint) = omq.bind(pull, <<"tcp://127.0.0.1:0">>)
  let assert Ok(Nil) = omq.connect(push, endpoint)
  let assert Ok(Nil) = omq.send_string(push, "hello")
  let assert Ok("hello") = omq.recv_string_timeout(pull, 1000)
  Nil
}
```

Development commands live in the parent OMQ.beam package.
