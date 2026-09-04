const std = @import("std");
const omq = @import("omq");

const c = @cImport({
    @cInclude("arpa/inet.h");
    @cInclude("netinet/in.h");
    @cInclude("sys/socket.h");
    @cInclude("unistd.h");
});

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

fn freeUdpPort() !u16 {
    const fd = c.socket(c.AF_INET, c.SOCK_DGRAM, 0);
    try testing.expect(fd >= 0);
    defer _ = c.close(fd);

    var addr: c.struct_sockaddr_in = std.mem.zeroes(c.struct_sockaddr_in);
    addr.sin_family = c.AF_INET;
    addr.sin_port = c.htons(0);
    addr.sin_addr.s_addr = c.htonl(0x7f000001);

    try testing.expectEqual(
        @as(c_int, 0),
        c.bind(fd, @ptrCast(&addr), @sizeOf(c.struct_sockaddr_in)),
    );

    var len: c.socklen_t = @sizeOf(c.struct_sockaddr_in);
    try testing.expectEqual(
        @as(c_int, 0),
        c.getsockname(fd, @ptrCast(&addr), &len),
    );
    return c.ntohs(addr.sin_port);
}

fn expectRecv(socket: *omq.Socket, expected: []const u8) !void {
    const got = try socket.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualStrings(expected, got);
}

fn configurePlain(server: *omq.Socket, client: *omq.Socket) !void {
    const credentials = [_]omq.PlainCredential{
        .{ .username = "alice", .password = "secret" },
        .{ .username = "bob", .password = "hunter2" },
    };
    try server.setPlainServerCredentials(allocator, &credentials);
    try client.setPlainClient(allocator, "alice", "secret");
}

fn configureCurve(server_sock: *omq.Socket, client_sock: *omq.Socket) !void {
    const server = try omq.curveKeypair();
    const client = try omq.curveKeypair();
    try server_sock.setCurveServer(allocator, server.publicSlice(), server.secretSlice());
    try client_sock.setCurveClient(allocator, client.publicSlice(), client.secretSlice(), server.publicSlice());
}

const ProxyState = struct {
    frontend: *omq.Socket,
    backend: *omq.Socket,
    control: *omq.Socket,
    failed: *bool,
};

fn proxySteerableThread(state: *ProxyState) void {
    omq.proxySteerable(state.frontend, state.backend, null, state.control) catch {
        state.failed.* = true;
    };
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

test "named option helpers reject invalid values" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var sock = try ctx.socket(omq.PUSH);
    defer sock.deinit();

    try sock.setSendHighWaterMark(64);
    try sock.setReceiveHighWaterMark(32);
    try testing.expectError(error.Invalid, sock.setSendHighWaterMark(-1));
    try testing.expectError(error.Invalid, sock.setReceiveHighWaterMark(-1));
    try testing.expectEqual(@as(i32, 64), try sock.sendHighWaterMark());
    try testing.expectEqual(@as(i32, 32), try sock.receiveHighWaterMark());

    try testing.expectError(error.Invalid, sock.setOnMute(99));
    try testing.expectError(error.Invalid, sock.setCompressionLevel(-9));
    try testing.expectError(error.Invalid, sock.setCompressionLevel(5));

    const oversize = try allocator.alloc(u8, 8 * 1024 + 1);
    defer allocator.free(oversize);
    @memset(oversize, 0);
    try testing.expectError(error.Invalid, sock.setCompressionDict(oversize));
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

    try testing.expect(try pull.fd() >= 0);
    try testing.expectEqual(@as(i16, 0), try pull.poll(20, omq.POLLIN));
    _ = try push.send("ready", 0);
    try testing.expect((try pull.poll(1000, omq.POLLIN)) & omq.POLLIN != 0);
    try testing.expect((try pull.pollEvents()) & omq.POLLIN != 0);
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

test "connect before bind works for ipc push pull" {
    if (!try omq.has(allocator, "ipc")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();
    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();

    const path_raw = try std.fmt.allocPrint(allocator, "/tmp/omq-zig-cbb-{d}", .{c.getpid()});
    defer allocator.free(path_raw);
    const path = try allocator.dupeZ(u8, path_raw);
    defer allocator.free(path);
    _ = c.unlink(path.ptr);
    defer _ = c.unlink(path.ptr);

    const endpoint = try std.fmt.allocPrint(allocator, "ipc://{s}", .{path});
    defer allocator.free(endpoint);

    try pull.setReceiveTimeout(2000);
    try push.setSendTimeout(2000);
    try push.connect(allocator, endpoint);
    sleepMillis(20);
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);

    _ = try push.send("ipc-cbb", 0);
    try expectRecv(&pull, "ipc-cbb");
}

test "connect before bind works for tcp push pull" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var probe = try ctx.socket(omq.PULL);
    const port = try probe.bindToRandomPort(allocator, "tcp://127.0.0.1", .{});
    try probe.close();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();
    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();

    const endpoint = try std.fmt.allocPrint(allocator, "tcp://127.0.0.1:{d}", .{port});
    defer allocator.free(endpoint);

    try pull.setReceiveTimeout(5000);
    try push.setSendTimeout(5000);
    try push.connect(allocator, endpoint);
    sleepMillis(20);
    const bound = try pull.bind(allocator, endpoint);
    defer allocator.free(bound);

    _ = try push.send("tcp-cbb", 0);
    try expectRecv(&pull, "tcp-cbb");
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

test "proxy steerable forwards and terminates" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var frontend = try ctx.socket(omq.PULL);
    defer frontend.deinit();
    var backend = try ctx.socket(omq.PUSH);
    defer backend.deinit();
    var control = try ctx.socket(omq.PULL);
    defer control.deinit();
    var sender = try ctx.socket(omq.PUSH);
    defer sender.deinit();
    var receiver = try ctx.socket(omq.PULL);
    defer receiver.deinit();
    var controller = try ctx.socket(omq.PUSH);
    defer controller.deinit();

    try frontend.allowThreadMigration();
    try backend.allowThreadMigration();
    try control.allowThreadMigration();

    const frontend_endpoint = try frontend.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(frontend_endpoint);
    const backend_endpoint = try backend.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(backend_endpoint);
    const control_endpoint = try control.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(control_endpoint);

    try sender.connect(allocator, frontend_endpoint);
    try receiver.connect(allocator, backend_endpoint);
    try controller.connect(allocator, control_endpoint);
    try receiver.setReceiveTimeout(2000);

    var failed = false;
    var state: ProxyState = .{
        .frontend = &frontend,
        .backend = &backend,
        .control = &control,
        .failed = &failed,
    };
    const thread = try std.Thread.spawn(.{}, proxySteerableThread, .{&state});
    sleepMillis(50);

    _ = try sender.send("through-proxy", 0);
    try expectRecv(&receiver, "through-proxy");
    _ = try controller.send("TERMINATE", 0);
    thread.join();
    try testing.expect(!failed);
}

test "stream socket accepts raw tcp" {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var stream = try ctx.socket(omq.STREAM);
    defer stream.deinit();
    try stream.setReceiveTimeout(2000);

    const port = try stream.bindToRandomPort(allocator, "tcp://127.0.0.1", .{});

    const fd = c.socket(c.AF_INET, c.SOCK_STREAM, 0);
    try testing.expect(fd >= 0);
    defer _ = c.close(fd);

    var addr: c.struct_sockaddr_in = std.mem.zeroes(c.struct_sockaddr_in);
    addr.sin_family = c.AF_INET;
    addr.sin_port = c.htons(port);
    addr.sin_addr.s_addr = c.htonl(0x7f000001);

    try testing.expectEqual(
        @as(c_int, 0),
        c.connect(
            fd,
            @ptrCast(&addr),
            @sizeOf(c.struct_sockaddr_in),
        ),
    );

    try testing.expectEqual(@as(isize, 5), c.send(fd, "hello", 5, 0));

    var opened = try stream.recvMultipartAlloc(allocator, 0);
    defer opened.deinit();
    try testing.expectEqual(@as(usize, 2), opened.parts.len);
    try testing.expect(opened.parts[0].len > 0);
    try testing.expectEqualStrings("", opened.parts[1]);

    var data = try stream.recvMultipartAlloc(allocator, 0);
    defer data.deinit();
    try testing.expectEqual(@as(usize, 2), data.parts.len);
    try testing.expectEqualSlices(u8, opened.parts[0], data.parts[0]);
    try testing.expectEqualStrings("hello", data.parts[1]);

    const reply = [_][]const u8{ data.parts[0], "world" };
    try stream.sendMultipart(&reply, 0);

    var buffer: [16]u8 = undefined;
    const received = c.recv(fd, &buffer, buffer.len, 0);
    try testing.expectEqual(@as(isize, 5), received);
    try testing.expectEqualStrings("world", buffer[0..5]);
}

test "radio dish udp filters groups" {
    if (!try omq.has(allocator, "udp")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var dish = try ctx.socket(omq.DISH);
    defer dish.deinit();
    var radio = try ctx.socket(omq.RADIO);
    defer radio.deinit();

    const port = try freeUdpPort();
    const endpoint = try std.fmt.allocPrint(allocator, "udp://127.0.0.1:{d}", .{port});
    defer allocator.free(endpoint);

    try dish.setReceiveTimeout(500);
    try dish.join(allocator, "weather");
    const bound = try dish.bind(allocator, endpoint);
    defer allocator.free(bound);
    try radio.connect(allocator, endpoint);
    sleepMillis(50);

    try radio.sendGroup(allocator, "news", "ignored", 0);
    try radio.sendGroup(allocator, "weather", "sunny", 0);

    var got = try dish.recvFrameAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqualStrings("sunny", got.data);

    try dish.leave(allocator, "weather");
    try radio.sendGroup(allocator, "weather", "ignored", 0);
    try testing.expectError(error.Again, dish.recvAlloc(allocator, 0));
}

test "plain req rep tcp" {
    if (!try omq.has(allocator, "plain")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
    defer req.deinit();

    try configurePlain(&rep, &req);
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

test "plain push pull tcp" {
    if (!try omq.has(allocator, "plain")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();
    var bob = try ctx.socket(omq.PUSH);
    defer bob.deinit();

    try configurePlain(&pull, &push);
    try bob.setPlainClient(allocator, "bob", "hunter2");
    try pull.setReceiveTimeout(5000);
    try push.setSendTimeout(5000);
    try bob.setSendTimeout(5000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);
    try bob.connect(allocator, bound);

    _ = try push.send("hello over plain", 0);
    try expectRecv(&pull, "hello over plain");
    _ = try bob.send("hello from bob", 0);
    try expectRecv(&pull, "hello from bob");
}

test "plain pub sub tcp" {
    if (!try omq.has(allocator, "plain")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var publisher = try ctx.socket(omq.PUB);
    defer publisher.deinit();
    var sub = try ctx.socket(omq.SUB);
    defer sub.deinit();

    try configurePlain(&publisher, &sub);
    try sub.subscribe("hot/");
    try sub.setReceiveTimeout(5000);

    const bound = try publisher.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try sub.connect(allocator, bound);
    sleepMillis(300);

    _ = try publisher.send("cold/skip", 0);
    _ = try publisher.send("hot/take", 0);
    try expectRecv(&sub, "hot/take");
}

test "plain multipart tcp" {
    if (!try omq.has(allocator, "plain")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try configurePlain(&pull, &push);
    try pull.setReceiveTimeout(5000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    try push.sendMultipart(&.{ "a", "bb", "ccc" }, 0);
    var got = try pull.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqual(@as(usize, 3), got.parts.len);
    try testing.expectEqualStrings("a", got.parts[0]);
    try testing.expectEqualStrings("bb", got.parts[1]);
    try testing.expectEqualStrings("ccc", got.parts[2]);
}

test "curve req rep tcp" {
    if (!try omq.has(allocator, "curve")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
    defer req.deinit();

    try configureCurve(&rep, &req);
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

test "curve push pull tcp" {
    if (!try omq.has(allocator, "curve")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try configureCurve(&pull, &push);
    try pull.setReceiveTimeout(5000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("hello over curve", 0);
    try expectRecv(&pull, "hello over curve");
}

test "curve pub sub tcp" {
    if (!try omq.has(allocator, "curve")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var publisher = try ctx.socket(omq.PUB);
    defer publisher.deinit();
    var sub = try ctx.socket(omq.SUB);
    defer sub.deinit();

    try configureCurve(&publisher, &sub);
    try sub.subscribe("hot/");
    try sub.setReceiveTimeout(5000);

    const bound = try publisher.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try sub.connect(allocator, bound);
    sleepMillis(300);

    _ = try publisher.send("cold/skip", 0);
    _ = try publisher.send("hot/take", 0);
    try expectRecv(&sub, "hot/take");
}

test "curve multipart tcp" {
    if (!try omq.has(allocator, "curve")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try configureCurve(&pull, &push);
    try pull.setReceiveTimeout(5000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    try push.sendMultipart(&.{ "a", "bb", "ccc" }, 0);
    var got = try pull.recvMultipartAlloc(allocator, 0);
    defer got.deinit();
    try testing.expectEqual(@as(usize, 3), got.parts.len);
    try testing.expectEqualStrings("a", got.parts[0]);
    try testing.expectEqualStrings("bb", got.parts[1]);
    try testing.expectEqualStrings("ccc", got.parts[2]);
}

test "curve bad server key rejects" {
    if (!try omq.has(allocator, "curve")) return;

    const server = try omq.curveKeypair();
    const wrong = try omq.curveKeypair();
    const client = try omq.curveKeypair();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try pull.setCurveServer(allocator, server.publicSlice(), server.secretSlice());
    try push.setCurveClient(allocator, client.publicSlice(), client.secretSlice(), wrong.publicSlice());
    try pull.setReceiveTimeout(1000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("should not arrive", 0);
    try testing.expectError(error.Again, pull.recvAlloc(allocator, 0));
}

test "curve mismatched keypair returns invalid" {
    if (!try omq.has(allocator, "curve")) return;

    const first = try omq.curveKeypair();
    const second = try omq.curveKeypair();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try push.setCurveClient(
        allocator,
        first.publicSlice(),
        second.secretSlice(),
        first.publicSlice(),
    );
    try testing.expectError(error.Invalid, push.connect(allocator, "tcp://127.0.0.1:1"));
}

test "curve option helpers round trip" {
    if (!try omq.has(allocator, "curve")) return;

    const keys = try omq.curveKeypair();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var sock = try ctx.socket(omq.PUSH);
    defer sock.deinit();

    try sock.setCurveServer(allocator, keys.publicSlice(), keys.secretSlice());
    try testing.expect(try sock.curveServer());

    try sock.setCurvePublicKey(allocator, keys.publicSlice());
    try sock.setCurveSecretKey(allocator, keys.secretSlice());
    const public = try sock.curvePublicKeyAlloc(allocator);
    defer allocator.free(public);
    try testing.expectEqualStrings(keys.publicSlice(), public);

    const secret = try sock.curveSecretKeyAlloc(allocator);
    defer allocator.free(secret);
    try testing.expectEqualStrings(keys.secretSlice(), secret);

    try sock.setCurveServerKey(allocator, keys.publicSlice());
    const server_key = try sock.curveServerKeyAlloc(allocator);
    defer allocator.free(server_key);
    try testing.expectEqualStrings(keys.publicSlice(), server_key);
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

test "zstd tcp custom level" {
    if (!try omq.has(allocator, "zstd")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try pull.setReceiveTimeout(2000);
    try push.setCompressionLevel(3);
    try testing.expectEqual(@as(i32, 3), try push.compressionLevel());

    const bound = try pull.bind(allocator, "zstd+tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    _ = try push.send("compressed-level", 0);
    try expectRecv(&pull, "compressed-level");
}

test "zstd tcp auto train" {
    if (!try omq.has(allocator, "zstd")) return;

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    try pull.setReceiveTimeout(2000);
    try push.setCompressionAutoTrain(true);
    try testing.expect(try push.compressionAutoTrain());

    const bound = try pull.bind(allocator, "zstd+tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const payload = try allocator.alloc(u8, 2048);
    defer allocator.free(payload);
    @memset(payload, 'z');

    _ = try push.send(payload, 0);
    const got = try pull.recvAlloc(allocator, 0);
    defer allocator.free(got);
    try testing.expectEqualSlices(u8, payload, got);
}
