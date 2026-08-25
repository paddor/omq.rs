#!/usr/bin/env elixir

defmodule OmqBenchPeer do
  def main([bench, impl, role, endpoint, size_text, duration_text, warmup_text]) do
    root = Path.expand("..", __DIR__)
    add_paths(root)
    ensure_impl(impl)

    size = String.to_integer(size_text)
    duration_ms = seconds_to_ms(duration_text)
    warmup_ms = seconds_to_ms(warmup_text)
    payload = :binary.copy("x", size)

    case {bench, impl, role} do
      {"pushpull", "omq-elixir", "pull"} ->
        pull(endpoint, payload, duration_ms, warmup_ms)

      {"pushpull", "omq-elixir", "push"} ->
        push(endpoint, payload)

      {"reqrep", "omq-elixir", "rep"} ->
        rep(endpoint)

      {"reqrep", "omq-elixir", "req"} ->
        req(endpoint, payload, duration_ms, warmup_ms)

      {"pushpull", impl, "pull"} when impl in ["erlzmq", "chumak"] ->
        pull(impl, endpoint, payload, duration_ms, warmup_ms)

      {"pushpull", impl, "push"} when impl in ["erlzmq", "chumak"] ->
        push(impl, endpoint, payload)

      {"reqrep", impl, "rep"} when impl in ["erlzmq", "chumak"] ->
        rep(impl, endpoint)

      {"reqrep", impl, "req"} when impl in ["erlzmq", "chumak"] ->
        req(impl, endpoint, payload, duration_ms, warmup_ms)

      _ ->
        die("bad benchmark args")
    end
  end

  def main(_args) do
    die(
      "usage: bench_peer.exs <pushpull|reqrep> <omq-elixir|erlzmq|chumak> <push|pull|req|rep> <endpoint> <size> <duration> <warmup>"
    )
  end

  defp add_paths(root) do
    Code.prepend_path(Path.join([root, "_build", "default", "lib", "omq", "ebin"]))
    Code.prepend_path(Path.join([root, "_build", "test", "lib", "omq", "ebin"]))
    Code.prepend_path(Path.join([root, "elixir", "_build", "dev", "lib", "omq_elixir", "ebin"]))
  end

  defp seconds_to_ms(text), do: round(String.to_float(text) * 1000)
  defp now_ms, do: System.monotonic_time(:millisecond)
  defp deadline(ms), do: now_ms() + ms

  defp ensure_impl("erlzmq") do
    Mix.install([{:erlzmq, "~> 4.2", hex: :erlzmq_dnif}], verbose: false)
  end

  defp ensure_impl("chumak") do
    Mix.install([{:chumak, "~> 1.5"}], verbose: false)
    {:ok, _apps} = Application.ensure_all_started(:chumak)
    :ok
  end

  defp ensure_impl(_impl), do: :ok

  defp open("omq-elixir", type) do
    {:ok, ctx} = OMQ.context()
    {:ok, sock} = OMQ.socket(ctx, type)
    {:omq_elixir, ctx, sock}
  end

  defp open("erlzmq", type) do
    {:ok, ctx} = erlzmq(:context, [])
    {:ok, sock} = erlzmq(:socket, [ctx, type])
    erlzmq(:setsockopt, [sock, :linger, 0])
    {:erlzmq, ctx, sock}
  end

  defp open("chumak", type) do
    {:ok, sock} = chumak(:socket, [type])
    {:chumak, sock}
  end

  defp bind({:omq_elixir, _ctx, sock}, endpoint), do: OMQ.bind(sock, endpoint)
  defp bind({:erlzmq, _ctx, sock}, endpoint), do: erlzmq(:bind, [sock, endpoint])
  defp bind({:chumak, sock}, endpoint), do: chumak_bind(sock, endpoint)
  defp connect({:omq_elixir, _ctx, sock}, endpoint), do: OMQ.connect(sock, endpoint)
  defp connect({:erlzmq, _ctx, sock}, endpoint), do: erlzmq(:connect, [sock, endpoint])
  defp connect({:chumak, sock}, endpoint), do: chumak_connect(sock, endpoint)
  defp send_msg({:omq_elixir, _ctx, sock}, payload), do: OMQ.send(sock, payload)
  defp send_msg({:erlzmq, _ctx, sock}, payload), do: erlzmq(:send, [sock, payload])
  defp send_msg({:chumak, sock}, payload), do: chumak(:send, [sock, payload])
  defp send_fast({:omq_elixir, _ctx, sock}, payload), do: OMQ.try_send(sock, payload)
  defp send_fast(pair, payload), do: send_msg(pair, payload)
  defp recv_msg({:omq_elixir, _ctx, sock}), do: OMQ.recv(sock)
  defp recv_msg({:erlzmq, _ctx, sock}), do: erlzmq(:recv, [sock])
  defp recv_msg({:chumak, sock}), do: chumak(:recv, [sock])
  defp try_recv_msg({:omq_elixir, _ctx, sock}), do: OMQ.try_recv(sock)
  defp try_recv_msg(pair), do: recv_msg(pair)

  defp close({:omq_elixir, ctx, sock}) do
    OMQ.close(sock)
    OMQ.term(ctx)
  end

  defp close({:erlzmq, ctx, sock}) do
    erlzmq(:close, [sock])
    erlzmq(:term, [ctx])
  end

  defp close({:chumak, sock}) do
    chumak(:stop, [sock])
  end

  defp erlzmq(function, args), do: :erlang.apply(:erlzmq, function, args)
  defp chumak(function, args), do: :erlang.apply(:chumak, function, args)

  defp chumak_bind(sock, endpoint) do
    {host, port} = tcp_host_port(endpoint)
    chumak(:bind, [sock, :tcp, host, port])
  end

  defp chumak_connect(sock, endpoint) do
    {host, port} = tcp_host_port(endpoint)
    chumak(:connect, [sock, :tcp, host, port])
  end

  defp tcp_host_port("tcp://" <> rest) do
    [host, port] = String.split(rest, ":", parts: 2)
    {String.to_charlist(host), String.to_integer(port)}
  end

  defp await_first_message({:chumak, sock} = pair, size) do
    unblocker =
      spawn(fn ->
        Process.sleep(10_000)
        chumak(:unblock, [sock])
      end)

    try do
      case recv_msg(pair) do
        {:ok, msg} ->
          true = byte_size(msg) == size
          :ok

        {:error, :again} ->
          die("chumak peer did not deliver first message")

        error ->
          okish(error)
      end
    after
      Process.exit(unblocker, :kill)
    end
  end

  defp await_first_message(_pair, _size), do: :ok

  defp pull(endpoint, payload, duration_ms, warmup_ms) do
    pull("omq-elixir", endpoint, payload, duration_ms, warmup_ms)
  end

  defp pull(impl, endpoint, payload, duration_ms, warmup_ms) do
    sock = open(impl, :pull)
    okish(bind(sock, endpoint))
    IO.puts("READY #{endpoint}")
    await_first_message(sock, byte_size(payload))

    timed_drain_until(
      sock,
      byte_size(payload),
      deadline(warmup_ms),
      0,
      timer_check_interval(byte_size(payload)),
      timer_check_interval(byte_size(payload))
    )

    start = now_ms()

    count =
      timed_drain_until(
        sock,
        byte_size(payload),
        start + duration_ms,
        0,
        timer_check_interval(byte_size(payload)),
        timer_check_interval(byte_size(payload))
      )

    stop = now_ms()
    close(sock)
    result(impl, "throughput", byte_size(payload), count, (stop - start) / 1000)
  end

  defp push(endpoint, payload) do
    push("omq-elixir", endpoint, payload)
  end

  defp push(impl, endpoint, payload) do
    sock = open(impl, :push)
    okish(connect(sock, endpoint))
    push_loop(sock, payload)
  end

  defp rep(endpoint) do
    rep("omq-elixir", endpoint)
  end

  defp rep(impl, endpoint) do
    sock = open(impl, :rep)
    okish(bind(sock, endpoint))
    IO.puts("READY #{endpoint}")
    rep_loop(sock)
  end

  defp req(endpoint, payload, duration_ms, warmup_ms) do
    req("omq-elixir", endpoint, payload, duration_ms, warmup_ms)
  end

  defp req(impl, endpoint, payload, duration_ms, warmup_ms) do
    sock = open(impl, :req)
    okish(connect(sock, endpoint))
    req_until(sock, payload, deadline(warmup_ms), [])
    start = now_ms()
    samples = req_until(sock, payload, start + duration_ms, [])
    stop = now_ms()
    close(sock)

    latency_result(
      impl,
      byte_size(payload),
      length(samples),
      (stop - start) / 1000,
      samples
    )
  end

  defp timed_drain_until({:chumak, sock} = pair, size, deadline, count, _checks_left, _interval) do
    unblocker =
      spawn(fn ->
        Process.sleep(max(deadline - now_ms(), 0))
        chumak(:unblock, [sock])
      end)

    try do
      chumak_drain_until(pair, size, deadline, count)
    after
      Process.exit(unblocker, :kill)
    end
  end

  defp timed_drain_until(pair, size, deadline, count, checks_left, interval) do
    drain_until(pair, size, deadline, count, checks_left, interval)
  end

  defp chumak_drain_until(pair, size, deadline, count) do
    if now_ms() >= deadline do
      count
    else
      case recv_msg(pair) do
        {:ok, msg} ->
          true = byte_size(msg) == size
          chumak_drain_until(pair, size, deadline, count + 1)

        {:error, :again} ->
          count

        error ->
          okish(error)
          count
      end
    end
  end

  defp drain_until(pair, size, deadline, count, 0, interval) do
    if now_ms() >= deadline do
      count
    else
      drain_until(pair, size, deadline, count, interval, interval)
    end
  end

  defp drain_until(pair, size, deadline, count, checks_left, interval) do
    case try_recv_msg(pair) do
      {:ok, msg} ->
        true = byte_size(msg) == size
        drain_until(pair, size, deadline, count + 1, checks_left - 1, interval)

      {:error, :would_block, _reason} ->
        if now_ms() >= deadline do
          count
        else
          :erlang.yield()
          drain_until(pair, size, deadline, count, checks_left, interval)
        end
    end
  end

  defp timer_check_interval(size) when size <= 1024, do: 4096
  defp timer_check_interval(_size), do: 256

  defp push_loop(pair, payload) do
    case send_fast(pair, payload) do
      :ok ->
        :ok

      {:error, :would_block, _reason} ->
        :erlang.yield()

      {:error, :no_connected_peers} ->
        :erlang.yield()

      error ->
        okish(error)
    end

    push_loop(pair, payload)
  end

  defp rep_loop(pair) do
    {:ok, msg} = recv_msg(pair)
    okish(send_msg(pair, msg))
    rep_loop(pair)
  end

  defp req_until(pair, payload, deadline, samples) do
    if now_ms() >= deadline do
      samples
    else
      start = System.monotonic_time(:microsecond)
      okish(send_msg(pair, payload))
      {:ok, ^payload} = recv_msg(pair)
      stop = System.monotonic_time(:microsecond)
      req_until(pair, payload, deadline, [stop - start | samples])
    end
  end

  defp okish(:ok), do: :ok
  defp okish({:ok, _}), do: :ok
  defp okish({:error, reason}), do: die(inspect(reason))
  defp okish({:error, class, reason}), do: die("#{inspect(class)}: #{inspect(reason)}")

  defp result(impl, kind, size, count, seconds) do
    msgs_s = count / seconds
    gb_s = msgs_s * size / 1_000_000_000

    IO.puts(
      ~s(RESULT {"impl":"#{impl}","kind":"#{kind}","msg_size":#{size},"messages":#{count},"seconds":#{round_float(seconds, 6)},"msgs_s":#{round_float(msgs_s, 3)},"gb_s":#{round_float(gb_s, 6)}})
    )
  end

  defp latency_result(impl, size, count, seconds, samples) do
    sorted = Enum.sort(samples)
    p50 = percentile(sorted, 0.50)
    p99 = percentile(sorted, 0.99)

    IO.puts(
      ~s(RESULT {"impl":"#{impl}","kind":"latency","msg_size":#{size},"messages":#{count},"seconds":#{round_float(seconds, 6)},"p50_us":#{round_float(p50, 3)},"p99_us":#{round_float(p99, 3)}})
    )
  end

  defp round_float(value, precision), do: Float.round(value * 1.0, precision)

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    index = min(length(sorted), max(1, ceil(length(sorted) * p))) - 1
    Enum.at(sorted, index)
  end

  defp die(message) do
    IO.puts(:stderr, message)
    System.halt(1)
  end
end

OmqBenchPeer.main(System.argv())
