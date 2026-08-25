const std = @import("std");
const omq = @import("omq");

const stop = "__OMQ_ZIG_BENCH_STOP__";

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.page_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 2) return usage(init);

    if (std.mem.eql(u8, args[1], "throughput")) {
        if (args.len != 4) return usage(init);
        try runThroughputTcp(init, allocator, try parseUsize(args[2]), try parseSeconds(args[3]));
        return;
    }
    if (std.mem.eql(u8, args[1], "throughput-push")) {
        if (args.len != 4) return usage(init);
        try runThroughputPush(init, allocator, try parseUsize(args[2]), try parseSeconds(args[3]));
        return;
    }
    if (std.mem.eql(u8, args[1], "throughput-pull")) {
        if (args.len != 4) return usage(init);
        try runThroughputPull(init, allocator, args[2], try parseUsize(args[3]));
        return;
    }
    if (std.mem.eql(u8, args[1], "latency")) {
        if (args.len != 6) return usage(init);
        try runLatency(init, allocator, try parseUsize(args[2]), try parseSeconds(args[3]), try parseSeconds(args[4]), args[5]);
        return;
    }
    return usage(init);
}

fn runThroughputTcp(init: std.process.Init, allocator: std.mem.Allocator, size: usize, duration_s: f64) !void {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:0");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    var sender: Sender = .{
        .socket = &push,
        .payload = payload,
        .duration_ns = secondsToNanos(duration_s),
    };
    const thread = try std.Thread.spawn(.{}, sendLoop, .{&sender});

    var count: u64 = 0;
    var start_ns: ?i128 = null;
    var recv_buffer = try allocator.alloc(u8, @max(size, stop.len));
    defer allocator.free(recv_buffer);
    while (true) {
        const received = try pull.recvInto(recv_buffer, 0);
        const msg = recv_buffer[0..received];
        if (std.mem.eql(u8, msg, stop)) break;
        if (start_ns == null) start_ns = nowNs();
        count += 1;
    }
    const end_ns = nowNs();
    thread.join();

    const elapsed = elapsedSeconds(start_ns orelse end_ns, end_ns);
    try printFloat(init, @as(f64, @floatFromInt(count)) / elapsed);
    std.process.exit(0);
}

fn runThroughputPush(init: std.process.Init, allocator: std.mem.Allocator, size: usize, duration_s: f64) !void {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try ctx.socket(omq.PUSH);
    defer push.deinit();
    try push.setLinger(0);

    const endpoint = try push.bind(allocator, "tcp://127.0.0.1:0");
    defer allocator.free(endpoint);
    try printLine(init, endpoint);

    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    const start = nowNs();
    const duration_ns = secondsToNanos(duration_s);
    while (nowNs() - start < duration_ns) {
        _ = try push.send(payload, 0);
    }
    _ = try push.send(stop, 0);
    waitForRelease();
    std.process.exit(0);
}

fn runThroughputPull(init: std.process.Init, allocator: std.mem.Allocator, endpoint: []const u8, size: usize) !void {
    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try ctx.socket(omq.PULL);
    defer pull.deinit();
    try pull.setLinger(0);
    try pull.connect(allocator, endpoint);

    var count: u64 = 0;
    var start_ns: ?i128 = null;
    var recv_buffer = try allocator.alloc(u8, @max(size, stop.len));
    defer allocator.free(recv_buffer);
    while (true) {
        const received = try pull.recvInto(recv_buffer, 0);
        const msg = recv_buffer[0..received];
        if (std.mem.eql(u8, msg, stop)) break;
        if (start_ns == null) start_ns = nowNs();
        count += 1;
    }
    const end_ns = nowNs();

    const elapsed = elapsedSeconds(start_ns orelse end_ns, end_ns);
    try printFloat(init, @as(f64, @floatFromInt(count)) / elapsed);
    std.process.exit(0);
}

fn runLatency(
    init: std.process.Init,
    allocator: std.mem.Allocator,
    size: usize,
    warmup_s: f64,
    duration_s: f64,
    endpoint: []const u8,
) !void {
    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var echo: Echoer = .{ .ctx = &ctx, .endpoint = endpoint, .allocator = allocator };
    const thread = try std.Thread.spawn(.{}, echoLoop, .{&echo});
    sleepMillis(50);

    var req = try ctx.socket(omq.REQ);
    defer req.deinit();
    try req.connect(allocator, endpoint);

    try pingLoop(&req, allocator, payload, secondsToNanos(warmup_s), null);

    var samples: std.array_list.Managed(f64) = .init(allocator);
    defer samples.deinit();
    try pingLoop(&req, allocator, payload, secondsToNanos(duration_s), &samples);

    _ = try req.send(stop, 0);
    const final = try req.recvAlloc(allocator, 0);
    allocator.free(final);
    thread.join();

    std.mem.sort(f64, samples.items, {}, comptime std.sort.asc(f64));
    const p50 = percentile(samples.items, 50);
    try printFloat(init, p50);
    std.process.exit(0);
}

const Sender = struct {
    socket: *omq.Socket,
    payload: []const u8,
    duration_ns: u64,
};

fn sendLoop(sender: *Sender) !void {
    const start = nowNs();
    while (nowNs() - start < sender.duration_ns) {
        _ = try sender.socket.send(sender.payload, 0);
    }
    _ = try sender.socket.send(stop, 0);
}

const Echoer = struct {
    ctx: *omq.Context,
    endpoint: []const u8,
    allocator: std.mem.Allocator,
};

fn echoLoop(echoer: *Echoer) !void {
    var socket = try echoer.ctx.socket(omq.REP);
    defer socket.deinit();
    const bound = try socket.bind(echoer.allocator, echoer.endpoint);
    defer echoer.allocator.free(bound);

    var recv_buffer = try echoer.allocator.alloc(u8, 1024 * 1024);
    defer echoer.allocator.free(recv_buffer);
    while (true) {
        const received = try socket.recvInto(recv_buffer, 0);
        const msg = recv_buffer[0..received];
        _ = try socket.send(msg, 0);
        if (std.mem.eql(u8, msg, stop)) {
            break;
        }
    }
}

fn pingLoop(
    req: *omq.Socket,
    allocator: std.mem.Allocator,
    payload: []const u8,
    duration_ns: u64,
    samples: ?*std.array_list.Managed(f64),
) !void {
    const recv_buffer = try allocator.alloc(u8, @max(payload.len, stop.len));
    defer allocator.free(recv_buffer);
    const start = nowNs();
    while (nowNs() - start < duration_ns) {
        const t0 = nowNs();
        _ = try req.send(payload, 0);
        _ = try req.recvInto(recv_buffer, 0);
        if (samples) |out| {
            try out.append(elapsedSeconds(t0, nowNs()) * 1_000_000.0);
        }
    }
}

fn parseUsize(raw: []const u8) !usize {
    return std.fmt.parseInt(usize, raw, 10);
}

fn parseSeconds(raw: []const u8) !f64 {
    return std.fmt.parseFloat(f64, raw);
}

fn secondsToNanos(seconds: f64) u64 {
    return @intFromFloat(seconds * std.time.ns_per_s);
}

fn elapsedSeconds(start_ns: i128, end_ns: i128) f64 {
    return @as(f64, @floatFromInt(end_ns - start_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
}

fn nowNs() i128 {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    return @as(i128, ts.sec) * std.time.ns_per_s + ts.nsec;
}

fn waitForRelease() void {
    var buffer: [1]u8 = undefined;
    _ = std.posix.read(0, &buffer) catch 0;
}

fn percentile(sorted: []const f64, pct: usize) f64 {
    if (sorted.len == 0) return 0.0;
    return sorted[sorted.len * pct / 100];
}

fn sleepMillis(ms: u64) void {
    const request: std.c.timespec = .{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    _ = std.c.nanosleep(&request, null);
}

fn printFloat(init: std.process.Init, value: f64) !void {
    var buffer: [256]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print("{d:.6}\n", .{value});
    try stdout.flush();
}

fn printLine(init: std.process.Init, value: []const u8) !void {
    var buffer: [256]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print("{s}\n", .{value});
    try stdout.flush();
}

fn usage(init: std.process.Init) !void {
    var buffer: [256]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(init.io, &buffer);
    const stderr = &stderr_writer.interface;
    try stderr.print("usage: omq_bench throughput SIZE SECONDS | throughput-push SIZE SECONDS | throughput-pull ENDPOINT SIZE | latency SIZE WARMUP_SECONDS SECONDS ENDPOINT\n", .{});
    try stderr.flush();
    return error.InvalidArgs;
}
