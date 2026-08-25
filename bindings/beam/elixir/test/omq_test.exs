defmodule OMQTest do
  use ExUnit.Case, async: false

  defp endpoint(name) do
    id = System.unique_integer([:positive, :monotonic])
    "inproc://elixir-#{name}-#{id}"
  end

  test "metadata helpers" do
    assert {:ok, "tokio"} = OMQ.backend_name()
    assert {:ok, version} = OMQ.version()
    assert is_binary(version)
    assert {:ok, ^version} = OMQ.omq_version()
    assert {:ok, {major, minor, patch}} = OMQ.omq_version_info()
    assert is_integer(major)
    assert is_integer(minor)
    assert is_integer(patch)
    assert "4.3.4" = OMQ.zmq_version()
    assert {4, 3, 4} = OMQ.zmq_version_info()
    assert is_binary(OMQ.strerror(11))
  end

  test "context singleton" do
    assert {:ok, a} = OMQ.context_instance()
    assert {:ok, b} = OMQ.instance()
    assert {:ok, key} = OMQ.context_share_key(a)
    assert {:ok, ^key} = OMQ.context_share_key(b)
    assert :ok = OMQ.term(a)
  end

  test "json and term roundtrip" do
    assert {:ok, ctx} = OMQ.context()
    assert {:ok, pull} = OMQ.socket(ctx, :pull)
    assert {:ok, push} = OMQ.socket(ctx, :push)
    ep = endpoint("json-term")
    assert {:ok, ^ep} = OMQ.bind(pull, ep)
    assert :ok = OMQ.connect(push, ep)

    json_value = %{"symbol" => "OMQ", "price" => 42}
    assert :ok = OMQ.send_json(push, json_value)
    assert {:ok, ^json_value} = OMQ.recv_json(pull, 1000)

    term = {:ok, [:beam, 42]}
    assert :ok = OMQ.send_term(push, term)
    assert {:ok, ^term} = OMQ.recv_term(pull, 1000)

    assert :ok = OMQ.close(push)
    assert :ok = OMQ.close(pull)
    assert :ok = OMQ.term(ctx)
  end

  test "Elixir wrapper receives from Erlang API peer" do
    assert {:ok, ctx} = OMQ.context()
    assert {:ok, pull} = OMQ.socket(ctx, :pull)
    assert {:ok, push} = :omq.socket(ctx, :push)
    ep = endpoint("erlang-to-elixir")
    assert {:ok, ^ep} = OMQ.bind(pull, ep)
    assert :ok = :omq.connect(push, ep)
    assert :ok = :omq.send(push, <<"from-erlang-api">>)
    assert {:ok, "from-erlang-api"} = OMQ.recv_string(pull, 1000)
    assert :ok = :omq.close(push)
    assert :ok = OMQ.close(pull)
    assert :ok = OMQ.term(ctx)
  end

  test "Erlang API receives from Elixir wrapper peer" do
    assert {:ok, ctx} = OMQ.context()
    assert {:ok, pull} = :omq.socket(ctx, :pull)
    assert {:ok, push} = OMQ.socket(ctx, :push)
    ep = endpoint("elixir-to-erlang")
    assert {:ok, ^ep} = :omq.bind(pull, ep)
    assert :ok = OMQ.connect(push, ep)
    assert :ok = OMQ.send_json(push, %{"from" => "elixir"})
    assert {:ok, %{"from" => "elixir"}} = :omq.recv_json(pull, 1000)
    assert :ok = OMQ.close(push)
    assert :ok = :omq.close(pull)
    assert :ok = OMQ.term(ctx)
  end
end
