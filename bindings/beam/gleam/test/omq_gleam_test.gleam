import gleam/bit_array
import gleeunit
import gleeunit/should
import omq_gleam as omq

type ErlangPeer

pub fn main() {
  gleeunit.main()
}

@external(erlang, "omq_gleam_test_ffi", "ensure_omq")
fn ensure_omq() -> Nil

@external(erlang, "omq_gleam_test_ffi", "endpoint")
fn endpoint(name: String) -> BitArray

@external(erlang, "omq_gleam_test_ffi", "erlang_push_send")
fn erlang_push_send(
  context: omq.Context,
  endpoint: BitArray,
  body: BitArray,
) -> Nil

@external(erlang, "omq_gleam_test_ffi", "erlang_start_pull")
fn erlang_start_pull(context: omq.Context, endpoint: BitArray) -> ErlangPeer

@external(erlang, "omq_gleam_test_ffi", "erlang_wait_pull")
fn erlang_wait_pull(peer: ErlangPeer) -> BitArray

pub fn metadata_test() {
  ensure_omq()
  let assert Ok(<<"tokio":utf8>>) = omq.backend_name()
  let assert Ok(version) = omq.version()
  let assert Ok(same_version) = omq.omq_version()
  same_version |> should.equal(version)
  let assert Ok(#(major, minor, patch)) = omq.omq_version_info()
  let assert True = major >= 0
  let assert True = minor >= 0
  let assert True = patch >= 0
  omq.zmq_version() |> should.equal(<<"4.3.4":utf8>>)
  omq.zmq_version_info() |> should.equal(#(4, 3, 4))
  let assert True = bit_array.byte_size(omq.strerror(11)) > 0
}

pub fn socket_roundtrip_test() {
  ensure_omq()
  let assert Ok(ctx) = omq.context()
  let assert Ok(pull) = omq.socket(ctx, omq.pull())
  let assert Ok(push) = omq.socket(ctx, omq.push())
  let ep = endpoint("push-pull")
  let assert Ok(ep) = omq.bind(pull, ep)
  let assert Ok(Nil) = omq.connect(push, ep)
  let assert Ok(Nil) = omq.send(push, <<"hello":utf8>>)
  let assert Ok(<<"hello":utf8>>) = omq.recv(pull)
  let assert Ok(Nil) = omq.close(push)
  let assert Ok(Nil) = omq.close(pull)
  let assert Ok(Nil) = omq.term(ctx)
}

pub fn singleton_test() {
  ensure_omq()
  let assert Ok(a) = omq.context_instance()
  let assert Ok(b) = omq.instance()
  let assert Ok(key_a) = omq.context_share_key(a)
  let assert Ok(key_b) = omq.context_share_key(b)
  key_b |> should.equal(key_a)
  let assert Ok(Nil) = omq.term(a)
}

pub fn receives_from_erlang_api_peer_test() {
  ensure_omq()
  let assert Ok(ctx) = omq.context()
  let assert Ok(pull) = omq.socket(ctx, omq.pull())
  let ep = endpoint("erlang-to-gleam")
  let assert Ok(ep) = omq.bind(pull, ep)
  erlang_push_send(ctx, ep, <<"from-erlang-api":utf8>>)
  let assert Ok(<<"from-erlang-api":utf8>>) = omq.recv(pull)
  let assert Ok(Nil) = omq.close(pull)
  let assert Ok(Nil) = omq.term(ctx)
}

pub fn erlang_api_receives_from_gleam_peer_test() {
  ensure_omq()
  let assert Ok(ctx) = omq.context()
  let assert Ok(push) = omq.socket(ctx, omq.push())
  let ep = endpoint("gleam-to-erlang")
  let peer = erlang_start_pull(ctx, ep)
  let assert Ok(Nil) = omq.connect(push, ep)
  let assert Ok(Nil) = omq.send(push, <<"from-gleam":utf8>>)
  erlang_wait_pull(peer) |> should.equal(<<"from-gleam":utf8>>)
  let assert Ok(Nil) = omq.close(push)
  let assert Ok(Nil) = omq.term(ctx)
}
