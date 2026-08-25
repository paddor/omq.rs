const std = @import("std");
const omq = @import("omq");

const testing = std.testing;
const allocator = testing.allocator;

fn sleepMillis(ms: u64) void {
    const request: std.c.timespec = .{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    _ = std.c.nanosleep(&request, null);
}

test "version reports libzmq ABI version" {
    const got = omq.version();
    try testing.expectEqual(@as(i32, 4), got.major);
    try testing.expectEqual(@as(i32, 3), got.minor);
}

test "push pull inproc" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const endpoint = "inproc://zig-basic";
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("hello", 0);
    const got = try pull.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings("hello", got);
}

test "construct every socket type" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    const types = [_]i32{
        omq.PAIR,
        omq.PUB,
        omq.SUB,
        omq.REQ,
        omq.REP,
        omq.DEALER,
        omq.ROUTER,
        omq.PULL,
        omq.PUSH,
        omq.XPUB,
        omq.XSUB,
        omq.STREAM,
        omq.SERVER,
        omq.CLIENT,
        omq.RADIO,
        omq.DISH,
        omq.GATHER,
        omq.SCATTER,
        omq.PEER,
        omq.CHANNEL,
    };

    for (types) |socket_type| {
        var socket = try ctx.socket(socket_type);
        try testing.expectEqual(socket_type, try socket.getInt(omq.TYPE));
        socket.deinit();
    }
}

test "pair sockets exchange both directions" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var a = try ctx.socket(omq.PAIR);
    defer a.deinit();
    var b = try ctx.socket(omq.PAIR);
    defer b.deinit();

    const bound = try a.bind(allocator, "inproc://zig-pair");
    defer allocator.free(bound);
    try b.connect(allocator, bound);

    _ = try a.send("left", 0);
    const got_left = try b.recvAlloc(allocator, 0);
    defer allocator.free(got_left);
    try testing.expectEqualStrings("left", got_left);

    _ = try b.send("right", 0);
    const got_right = try a.recvAlloc(allocator, 0);
    defer allocator.free(got_right);
    try testing.expectEqualStrings("right", got_right);
}

test "push pull multipart and rcvmore" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const endpoint = "inproc://zig-multipart";
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    try push.sendMultipart(&.{ "meta", "trailer" }, 0);

    var got = try pull.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqual(@as(usize, 2), got.parts.len);
    try testing.expectEqualStrings("meta", got.parts[0]);
    try testing.expectEqualStrings("trailer", got.parts[1]);
    try testing.expectEqual(@as(i32, 0), try pull.getInt(omq.RCVMORE));
}

test "sndmore aggregates frames" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const endpoint = "inproc://zig-sndmore";
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("a", omq.SNDMORE);
    _ = try push.send("b", omq.SNDMORE);
    _ = try push.send("c", 0);

    var got = try pull.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqual(@as(usize, 3), got.parts.len);
    try testing.expectEqualStrings("a", got.parts[0]);
    try testing.expectEqualStrings("b", got.parts[1]);
    try testing.expectEqualStrings("c", got.parts[2]);
}

test "scatter gather single frames" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var gather = try ctx.socket(omq.GATHER);
    defer gather.deinit();
    var scatter = try ctx.socket(omq.SCATTER);
    defer scatter.deinit();

    const bound = try gather.bind(allocator, "inproc://zig-scatter-gather");
    defer allocator.free(bound);
    try scatter.connect(allocator, bound);

    _ = try scatter.send("m0", 0);
    _ = try scatter.send("m1", 0);

    const first = try gather.recvAlloc(allocator, 0);
    defer allocator.free(first);
    const second = try gather.recvAlloc(allocator, 0);
    defer allocator.free(second);
    try testing.expectEqualStrings("m0", first);
    try testing.expectEqualStrings("m1", second);
}

test "channel sockets exchange single frames" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var a = try ctx.socket(omq.CHANNEL);
    defer a.deinit();
    var b = try ctx.socket(omq.CHANNEL);
    defer b.deinit();

    const bound = try a.bind(allocator, "inproc://zig-channel");
    defer allocator.free(bound);
    try b.connect(allocator, bound);

    _ = try a.send("hi", 0);
    const hi = try b.recvAlloc(allocator, 0);
    defer allocator.free(hi);
    try testing.expectEqualStrings("hi", hi);

    _ = try b.send("there", 0);
    const there = try a.recvAlloc(allocator, 0);
    defer allocator.free(there);
    try testing.expectEqualStrings("there", there);
}

test "req rep roundtrip" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
    defer req.deinit();

    const bound = try rep.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try req.connect(allocator, bound);

    _ = try req.send("ping", 0);
    const request = try rep.recvAlloc(allocator, 0);
    defer allocator.free(request);
    try testing.expectEqualStrings("ping", request);

    _ = try rep.send("pong", 0);
    const reply = try req.recvAlloc(allocator, 0);
    defer allocator.free(reply);
    try testing.expectEqualStrings("pong", reply);
}

test "peer sockets route by identity" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var a = try ctx.socket(omq.PEER);
    defer a.deinit();
    var b = try ctx.socket(omq.PEER);
    defer b.deinit();

    try a.setIdentity("peer-a");
    try b.setIdentity("peer-b");
    const bound = try a.bind(allocator, "inproc://zig-peer");
    defer allocator.free(bound);
    try b.connect(allocator, bound);
    try a.setSendTimeout(1000);
    try a.setReceiveTimeout(1000);
    try b.setSendTimeout(1000);
    try b.setReceiveTimeout(1000);
    sleepMillis(50);

    try b.sendMultipart(&.{ "peer-a", "hello-a" }, 0);
    var got_a = try a.recvMultipartAlloc(allocator, 0);
    defer got_a.deinit();
    try testing.expectEqual(@as(usize, 2), got_a.parts.len);
    try testing.expectEqualStrings("peer-b", got_a.parts[0]);
    try testing.expectEqualStrings("hello-a", got_a.parts[1]);

    try a.sendMultipart(&.{ "peer-b", "hello-b" }, 0);
    var got_b = try b.recvMultipartAlloc(allocator, 0);
    defer got_b.deinit();
    try testing.expectEqual(@as(usize, 2), got_b.parts.len);
    try testing.expectEqualStrings("peer-a", got_b.parts[0]);
    try testing.expectEqualStrings("hello-b", got_b.parts[1]);
}

test "dealer router identity routes back" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var router = try ctx.socket(omq.ROUTER);
    defer router.deinit();
    var dealer = try ctx.socket(omq.DEALER);
    defer dealer.deinit();

    try dealer.setIdentity("client-A");
    const bound = try router.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try dealer.connect(allocator, bound);

    _ = try dealer.send("hello", 0);
    var got = try router.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expect(got.parts.len >= 2);
    try testing.expectEqualStrings("client-A", got.parts[0]);
    try testing.expectEqualStrings("hello", got.parts[got.parts.len - 1]);

    try router.sendMultipart(&.{ "client-A", "hi-back" }, 0);
    const reply = try dealer.recvAlloc(allocator, 0);
    defer allocator.free(reply);
    try testing.expectEqualStrings("hi-back", reply);
}

test "client server routing id routes reply" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var server = try ctx.socket(omq.SERVER);
    defer server.deinit();
    var client = try ctx.socket(omq.CLIENT);
    defer client.deinit();

    const bound = try server.bind(allocator, "inproc://zig-client-server");
    defer allocator.free(bound);
    try client.connect(allocator, bound);

    _ = try client.send("ping", 0);
    var request = try server.recvFrameAlloc(allocator, 0);
    defer request.deinit();
    try testing.expectEqualStrings("ping", request.data);
    try testing.expect(request.routing_id > 0);

    var reply = try omq.Frame.init(allocator, "pong");
    defer reply.deinit();
    reply.routing_id = request.routing_id;
    try server.sendFrame(allocator, &reply, 0);

    const got = try client.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings("pong", got);
}

test "pub sub prefix filter" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var publisher = try ctx.socket(omq.PUB);
    defer publisher.deinit();
    var sub = try ctx.socket(omq.SUB);
    defer sub.deinit();

    const bound = try publisher.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try sub.connect(allocator, bound);
    try sub.subscribe("weather/");
    sleepMillis(200);

    _ = try publisher.send("sports/score-12", 0);
    _ = try publisher.send("weather/sunny", 0);
    _ = try publisher.send("weather/rain", 0);

    try sub.setReceiveTimeout(500);
    const first = try sub.recvAlloc(allocator, 0);
    defer allocator.free(first);
    const second = try sub.recvAlloc(allocator, 0);
    defer allocator.free(second);
    try testing.expectEqualStrings("weather/sunny", first);
    try testing.expectEqualStrings("weather/rain", second);
}

test "xpub xsub subscription and publish" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var xpub = try ctx.socket(omq.XPUB);
    defer xpub.deinit();
    var xsub = try ctx.socket(omq.XSUB);
    defer xsub.deinit();

    const bound = try xpub.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try xsub.connect(allocator, bound);
    try xsub.subscribe("");

    try xpub.setReceiveTimeout(1000);
    const subscription = try xpub.recvAlloc(allocator, 0);
    defer allocator.free(subscription);
    try testing.expectEqualSlices(u8, &[_]u8{1}, subscription);

    try xsub.setReceiveTimeout(1000);
    _ = try xpub.send("hello", 0);
    const got = try xsub.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings("hello", got);
}

test "option round trips" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try push.setLinger(50);
    try testing.expectEqual(@as(i32, 50), try push.getInt(omq.LINGER));

    try push.setSendHighWaterMark(64);
    try testing.expectEqual(@as(i32, 64), try push.getInt(omq.SNDHWM));

    try push.setReceiveHighWaterMark(32);
    try testing.expectEqual(@as(i32, 32), try push.getInt(omq.RCVHWM));
}

test "receive timeout maps to Again" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try pull.setReceiveTimeout(50);

    try testing.expectError(error.Again, pull.recvAlloc(allocator, 0));
}

test "closed socket maps to NoSocket" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    try pull.close();

    var buffer: [1]u8 = undefined;
    try testing.expectError(error.NoSocket, pull.recvInto(&buffer, 0));
}

test "extended option round trips" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try push.setInt(omq.SNDBUF, 65536);
    try testing.expectEqual(@as(i32, 65536), try push.getInt(omq.SNDBUF));

    try push.setInt(omq.RCVBUF, 32768);
    try testing.expectEqual(@as(i32, 32768), try push.getInt(omq.RCVBUF));

    try push.setRouterMandatory(true);
    try testing.expectEqual(@as(i32, 1), try push.getInt(omq.ROUTER_MANDATORY));

    try push.setConflate(true);
    try testing.expectEqual(@as(i32, 1), try push.getInt(omq.CONFLATE));

    try push.setArenaThreshold(2048);
    try testing.expectEqual(@as(i64, 2048), try push.arenaThreshold());

    try push.setOnMute(omq.OMQ_ON_MUTE_DROP_NEWEST);
    try testing.expectEqual(@as(i32, omq.OMQ_ON_MUTE_DROP_NEWEST), try push.getInt(omq.OMQ_ON_MUTE));

    try push.setCompressionLevel(1);
    try testing.expectEqual(@as(i32, 1), try push.getInt(omq.OMQ_COMPRESSION_LEVEL));

    try push.setCompressionAutoTrain(true);
    try testing.expectEqual(@as(i32, 1), try push.getInt(omq.OMQ_COMPRESSION_AUTO_TRAIN));

    try push.setCompressionDict("my-dict-bytes");
    const dict = try push.compressionDictAlloc(allocator);
    defer allocator.free(dict);
    try testing.expectEqualStrings("my-dict-bytes", dict);
}

test "identity and plain option bytes round trip" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try push.setIdentity("zig-id");
    const identity = try push.identityAlloc(allocator);
    defer allocator.free(identity);
    try testing.expectEqualStrings("zig-id", identity);

    try push.setPlainServer(true);
    try testing.expectEqual(@as(i32, 1), try push.getInt(omq.PLAIN_SERVER));

    try push.setPlainClient(allocator, "admin", "secret");
    const username = try push.getStringAlloc(allocator, omq.PLAIN_USERNAME, 64);
    defer allocator.free(username);
    const password = try push.getStringAlloc(allocator, omq.PLAIN_PASSWORD, 64);
    defer allocator.free(password);
    try testing.expectEqualStrings("admin", username);
    try testing.expectEqualStrings("secret", password);
    try testing.expectEqual(@as(i32, omq.PLAIN), try push.getInt(omq.MECHANISM));
}

test "poll reports readable socket" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const endpoint = "inproc://zig-poll";
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);
    try push.connect(allocator, bound);
    _ = try push.send("ready", 0);

    var items = [_]omq.PollItem{try pull.pollItem(omq.POLLIN)};
    try testing.expectEqual(@as(usize, 1), try omq.poll(items[0..], 1000));
    try testing.expect((items[0].revents & omq.POLLIN) != 0);
}

test "poller returns registered readable sockets" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull1 = try ctx.socket(omq.PULL);
    defer pull1.deinit();
    var push1 = try ctx.socket(omq.PUSH);
    defer push1.deinit();
    var pull2 = try ctx.socket(omq.PULL);
    defer pull2.deinit();
    var push2 = try ctx.socket(omq.PUSH);
    defer push2.deinit();

    const bound1 = try pull1.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound1);
    try push1.connect(allocator, bound1);

    const bound2 = try pull2.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound2);
    try push2.connect(allocator, bound2);

    _ = try push1.send("only-one", 0);

    var poller = omq.Poller.init(allocator);
    defer poller.deinit();
    try poller.register(&pull1, omq.POLLIN);
    try poller.register(&pull2, omq.POLLIN);

    const events = try poller.pollAlloc(1000);
    defer allocator.free(events);
    try testing.expectEqual(@as(usize, 1), events.len);
    try testing.expect(events[0].socket == &pull1);
    try testing.expect((events[0].events & omq.POLLIN) != 0);
}

test "monitor receives listening event" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();

    var monitor = try pull.monitor(&ctx, allocator, "inproc://zig-monitor", omq.EVENT_ALL);
    defer monitor.deinit();
    try monitor.setReceiveTimeout(1000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);

    var event = try monitor.recvAlloc(allocator, 0);
    defer event.deinit();
    try testing.expectEqual(@as(u16, omq.EVENT_LISTENING), event.event);
    try testing.expectEqualStrings(bound, event.endpoint);
}

test "shared contexts use same inproc namespace" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    const key = try ctx.shareKey();
    var shared = try omq.Context.fromShareKey(key);
    defer shared.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try shared.socket(omq.PUSH);
    defer push.deinit();

    const bound = try pull.bind(allocator, "inproc://zig-shared-context");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("shared", 0);
    const got = try pull.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings("shared", got);
}

test "curve key helpers" {
    if (!try omq.has(allocator, "curve")) return;

    const pair = try omq.curveKeypair();
    try testing.expectEqual(@as(usize, 40), pair.publicSlice().len);
    try testing.expectEqual(@as(usize, 40), pair.secretSlice().len);

    const public = try omq.curvePublic(allocator, pair.secretSlice());
    try testing.expectEqualSlices(u8, pair.publicSlice(), public[0..]);
}
