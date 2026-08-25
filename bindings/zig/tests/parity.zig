const std = @import("std");
const omq = @import("omq");

const testing = std.testing;
const allocator = testing.allocator;

const ZSTD_DICT = [_]u8{
    0x37, 0xa4, 0x30, 0xec, 0xbe, 0xaa, 0xdd, 0x5c, 0x81, 0x11, 0x20, 0x84,
    0x10, 0x42, 0x66, 0x46, 0x44, 0x44, 0x44, 0x42, 0x44, 0x90, 0x20, 0x02,
    0x11, 0x4c, 0x41, 0x86, 0x38, 0xc2, 0x18, 0x41, 0x04, 0x20, 0x82, 0x18,
    0x41, 0x04, 0x20, 0x82, 0x14, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    0x44, 0x24, 0x09, 0x00, 0x00, 0x51, 0x10, 0x63, 0x8c, 0x31, 0xc6, 0x18,
    0x63, 0x0c, 0x21, 0xc4, 0x18, 0x63, 0x66, 0x66, 0x86, 0x46, 0x92, 0x04,
    0x00, 0x80, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00,
};

fn sleepMillis(ms: u64) void {
    const request: std.c.timespec = .{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    _ = std.c.nanosleep(&request, null);
}

fn inprocEndpoint(comptime label: []const u8) []const u8 {
    return "inproc://zig-parity-" ++ label;
}

fn expectRecv(socket: *omq.Socket, expected: []const u8) !void {
    const got = try socket.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings(expected, got);
}

test "convenience methods cover pyomq behavior" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try push.setSendTimeout(1000);
    try testing.expectEqual(@as(i32, 1000), try push.sendTimeout());
    try push.setIdentity("alias-id");

    const bound = try pull.bind(allocator, inprocEndpoint("aliases"));
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.sendString("hello");
    try expectRecv(&pull, "hello");

    try push.sendMultipart(&.{ "a", "b", "c" }, 0);
    var got = try pull.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqual(@as(usize, 3), got.parts.len);
    try testing.expectEqualStrings("a", got.parts[0]);
    try testing.expectEqualStrings("b", got.parts[1]);
    try testing.expectEqualStrings("c", got.parts[2]);

    try push.close();
    try pull.close();
    try ctx.term();
}

test "socket lifecycle and option helpers match pyomq shape" {
    var ctx = try omq.Context.init();
    try testing.expect(!ctx.closed());

    var sock = try ctx.socket(omq.DEALER);
    try testing.expect(!sock.closed());

    try sock.setHighWaterMark(500);
    try testing.expectEqual(@as(i32, 500), try sock.highWaterMark());
    try testing.expectEqual(@as(i32, 500), try sock.sendHighWaterMark());
    try testing.expectEqual(@as(i32, 500), try sock.receiveHighWaterMark());

    try sock.setIdentity("dealer-a");
    const identity = try sock.identityAlloc(allocator);
    defer allocator.free(identity);
    try testing.expectEqualStrings("dealer-a", identity);

    try sock.close();
    try testing.expect(sock.closed());

    try ctx.term();
    try testing.expect(ctx.closed());
}

test "bindToRandomPort and getLastEndpoint" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var sock = try ctx.socket(omq.PULL);
    defer sock.deinit();

    const port = try sock.bindToRandomPort(allocator, "tcp://127.0.0.1", .{});
    try testing.expect(port >= 1024);

    const endpoint = try sock.getLastEndpoint(allocator);
    defer allocator.free(endpoint);
    try testing.expect(std.mem.startsWith(u8, endpoint, "tcp://127.0.0.1:"));

    var suffix_buf: [16]u8 = undefined;
    const suffix = try std.fmt.bufPrint(&suffix_buf, ":{d}", .{port});
    try testing.expect(std.mem.endsWith(u8, endpoint, suffix));

    var conflict = try ctx.socket(omq.PULL);
    defer conflict.deinit();
    try testing.expectError(
        error.AddressInUse,
        conflict.bindToRandomPort(allocator, "tcp://127.0.0.1", .{
            .min_port = port,
            .max_port = port,
            .max_tries = 1,
        }),
    );
}

test "rcvmore iterates multipart frames" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    try push.sendMultipart(&.{ "x", "y", "z" }, 0);
    try expectRecv(&pull, "x");
    try testing.expect(try pull.hasReceiveMore());
    try expectRecv(&pull, "y");
    try testing.expect(try pull.hasReceiveMore());
    try expectRecv(&pull, "z");
    try testing.expect(!try pull.hasReceiveMore());
}

test "socket poll helper reports timeout and readiness" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    try testing.expectEqual(@as(i16, 0), try pull.poll(20, omq.POLLIN));
    _ = try push.send("ready", 0);
    try testing.expect((try pull.poll(1000, omq.POLLIN)) & omq.POLLIN != 0);
    try expectRecv(&pull, "ready");
}

test "connect before bind works for inproc req rep" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var req = try ctx.socket(omq.REQ);
    defer req.deinit();
    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();

    const endpoint = inprocEndpoint("cbb-req-rep");
    try req.connect(allocator, endpoint);
    sleepMillis(20);
    const bound = try rep.bind(allocator, endpoint);
    defer allocator.free(bound);

    _ = try req.send("q", 0);
    try expectRecv(&rep, "q");
    _ = try rep.send("a", 0);
    try expectRecv(&req, "a");
}

test "connect before bind works for inproc pair" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var a = try ctx.socket(omq.PAIR);
    defer a.deinit();
    var b = try ctx.socket(omq.PAIR);
    defer b.deinit();

    const endpoint = inprocEndpoint("cbb-pair");
    try a.connect(allocator, endpoint);
    sleepMillis(20);
    const bound = try b.bind(allocator, endpoint);
    defer allocator.free(bound);

    _ = try a.send("from-a", 0);
    try expectRecv(&b, "from-a");
    _ = try b.send("from-b", 0);
    try expectRecv(&a, "from-b");
}

test "poller timeout unregister and modify" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    var poller = omq.Poller.init(allocator);
    defer poller.deinit();

    try poller.register(&pull, omq.POLLIN);
    const timeout_events = try poller.pollAlloc(20);
    defer allocator.free(timeout_events);
    try testing.expectEqual(@as(usize, 0), timeout_events.len);

    _ = try push.send("ready", 0);
    const events = try poller.pollAlloc(1000);
    defer allocator.free(events);
    try testing.expectEqual(@as(usize, 1), events.len);

    try poller.modify(&pull, 0);
    try expectRecv(&pull, "ready");
    const disabled_events = try poller.pollAlloc(20);
    defer allocator.free(disabled_events);
    try testing.expectEqual(@as(usize, 0), disabled_events.len);

    try poller.unregister(&pull);
    try testing.expectError(error.NoSocket, poller.modify(&pull, omq.POLLIN));
}

test "plain req rep tcp" {
    if (!try omq.has(allocator, "plain")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
    defer req.deinit();

    try rep.setPlainServer(true);
    try req.setPlainClient(allocator, "alice", "secret");
    try rep.setReceiveTimeout(5000);
    try req.setReceiveTimeout(5000);
    try req.setSendTimeout(5000);

    const bound = try rep.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try req.connect(allocator, bound);

    _ = try req.send("ping", 0);
    try expectRecv(&rep, "ping");
    _ = try rep.send("pong", 0);
    try expectRecv(&req, "pong");
}

test "curve req rep tcp" {
    if (!try omq.has(allocator, "curve")) return;

    const server = try omq.curveKeypair();
    const client = try omq.curveKeypair();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
    defer req.deinit();

    try rep.setCurveServer(allocator, server.publicSlice(), server.secretSlice());
    try req.setCurveClient(allocator, client.publicSlice(), client.secretSlice(), server.publicSlice());
    try rep.setReceiveTimeout(5000);
    try req.setReceiveTimeout(5000);
    try req.setSendTimeout(5000);

    const bound = try rep.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try req.connect(allocator, bound);

    _ = try req.send("ping", 0);
    try expectRecv(&rep, "ping");
    _ = try rep.send("pong", 0);
    try expectRecv(&req, "pong");
}

test "zstd tcp static dict" {
    if (!try omq.has(allocator, "zstd")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try pull.setReceiveTimeout(2000);
    try push.setCompressionLevel(1);
    try push.setCompressionDict(ZSTD_DICT[0..]);

    const bound = try pull.bind(allocator, "zstd+tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const payload = try allocator.alloc(u8, 4096);
    defer allocator.free(payload);
    @memset(payload, 'A');

    _ = try push.send(payload, 0);
    const got = try pull.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualSlices(u8, payload, got);
}
