require "./spec_helper"

describe OMQ do
  it "sends and receives over inproc" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 0, send_timeout: 1000)

    endpoint = pull.bind("inproc://crystal-basic")
    endpoint.should eq("inproc://crystal-basic")
    push.connect(endpoint).should be_true
    push.send("hello").should be_true
    pull.recv.should eq("hello")
    pull.try_recv.should be_nil

    push.close
    pull.close
    ctx.term
  end

  it "sends multipart messages" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 0, send_timeout: 1000)

    endpoint = pull.bind("inproc://crystal-multipart")
    push.connect(endpoint)
    push.send_parts(["one", "two", "three"])
    pull.recv_parts.should eq(["one", "two", "three"])

    push.close
    pull.close
    ctx.close
  end

  it "preserves binary payload bytes" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 0, send_timeout: 1000)

    endpoint = pull.bind("inproc://crystal-binary")
    push.connect(endpoint)
    payload = String.new(Bytes[0, 1, 2, 0, 255])
    push.send(payload)
    pull.recv.try(&.to_slice).should eq(payload.to_slice)

    push.close
    pull.close
    ctx.term
  end

  it "errors when bounded receive truncates a frame" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 0, send_timeout: 1000)

    endpoint = pull.bind("inproc://crystal-recv-limit")
    push.connect(endpoint)
    push.send("too-large")
    expect_raises(OMQ::Error, /receive limit/) do
      pull.recv(3)
    end

    push.close
    pull.close
    ctx.term
  end

  it "uses TCP wildcard endpoints" do
    ctx = OMQ.context
    pull = ctx.socket("pull", linger: 0, recv_timeout: 1000)
    push = ctx.socket("push", linger: 1000, send_timeout: 1000)

    endpoint = pull.bind("tcp://127.0.0.1:*")
    endpoint.starts_with?("tcp://").should be_true
    endpoint.includes?("*").should be_false
    push.connect(endpoint)
    push.send("tcp-ok")
    pull.recv.should eq("tcp-ok")

    push.close
    pull.close
    ctx.term
  end

  it "applies socket options" do
    ctx = OMQ.context
    socket = ctx.socket("push", linger: 0, send_timeout: 10, arena_threshold: 2048)

    socket.get_arena_threshold.should eq(2048)

    socket.close
    ctx.term
  end

  it "keeps context term explicit while sockets are live" do
    ctx = OMQ.context
    socket = ctx.socket("pull", linger: 0)

    expect_raises(OMQ::Error, /live sockets/) do
      ctx.term
    end

    socket.close.should be_true
    socket.close.should be_true
    ctx.term.should be_true
    expect_raises(OMQ::ClosedError) do
      ctx.socket("pull")
    end
  end

  it "supports pub/sub prefixes" do
    ctx = OMQ.context
    pub = ctx.socket("pub", linger: 0, send_timeout: 1000)
    sub = ctx.socket("sub", linger: 0, recv_timeout: 1000, subscribe: "topic")

    endpoint = pub.bind("inproc://crystal-pubsub")
    sub.connect(endpoint)
    sleep 50.milliseconds
    pub.send("topic hello")
    sub.recv.should eq("topic hello")

    sub.close
    pub.close
    ctx.term
  end
end
