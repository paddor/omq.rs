const std = @import("std");
const zimq = @import("zimq");

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
    const ctx: *zimq.Context = try .init();
    defer ctx.deinit();

    const pull: *zimq.Socket = try .init(ctx, .pull);
    defer pull.deinit();
    const push: *zimq.Socket = try .init(ctx, .push);
    defer push.deinit();

    try pull.bind("tcp://127.0.0.1:0");
    const endpoint = try lastEndpoint(allocator, pull);
    defer allocator.free(endpoint);
    try push.connect(endpoint);

    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    var sender: Sender = .{
        .socket = push,
        .payload = payload,
        .duration_ns = secondsToNanos(duration_s),
    };
    const thread = try std.Thread.spawn(.{}, sendLoop, .{&sender});

    var count: u64 = 0;
    var start_ns: ?i128 = null;
    while (true) {
        var msg: zimq.Message = .empty();
        _ = try pull.recvMsg(&msg, .{});
        const data = msg.slice();
        if (std.mem.eql(u8, data, stop)) {
            msg.deinit();
            break;
        }
        if (start_ns == null) start_ns = nowNs();
        count += 1;
        msg.deinit();
    }
    const end_ns = nowNs();
    thread.join();

    const elapsed = elapsedSeconds(start_ns orelse end_ns, end_ns);
    try printFloat(init, @as(f64, @floatFromInt(count)) / elapsed);
    std.process.exit(0);
}

fn runThroughputPush(init: std.process.Init, allocator: std.mem.Allocator, size: usize, duration_s: f64) !void {
    const ctx: *zimq.Context = try .init();
    defer ctx.deinit();

    const push: *zimq.Socket = try .init(ctx, .push);
    defer push.deinit();
    try push.set(.linger, 0);
    try push.bind("tcp://127.0.0.1:0");
    const endpoint = try lastEndpoint(allocator, push);
    defer allocator.free(endpoint);
    try printLine(init, endpoint);

    const payload = try allocator.alloc(u8, size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    const start = nowNs();
    const duration_ns = secondsToNanos(duration_s);
    while (nowNs() - start < duration_ns) {
        try push.sendSlice(payload, .{});
    }
    try push.sendSlice(stop, .{});
    waitForRelease();
    std.process.exit(0);
}

fn runThroughputPull(init: std.process.Init, allocator: std.mem.Allocator, endpoint: []const u8, size: usize) !void {
    const ctx: *zimq.Context = try .init();
    defer ctx.deinit();

    const pull: *zimq.Socket = try .init(ctx, .pull);
    defer pull.deinit();
    try pull.set(.linger, 0);
    const endpoint_z = try allocator.dupeZ(u8, endpoint);
    defer allocator.free(endpoint_z);
    try pull.connect(endpoint_z);

    _ = size;
    var count: u64 = 0;
    var start_ns: ?i128 = null;
    while (true) {
        var msg: zimq.Message = .empty();
        _ = try pull.recvMsg(&msg, .{});
        const data = msg.slice();
        if (std.mem.eql(u8, data, stop)) {
            msg.deinit();
            break;
        }
        if (start_ns == null) start_ns = nowNs();
        count += 1;
        msg.deinit();
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

    var echo: Echoer = .{ .endpoint = endpoint, .allocator = allocator };
    const thread = try std.Thread.spawn(.{}, echoLoop, .{&echo});
    sleepMillis(50);

    const ctx: *zimq.Context = try .init();
    defer ctx.deinit();
    const req: *zimq.Socket = try .init(ctx, .req);
    defer req.deinit();
    const endpoint_z = try allocator.dupeZ(u8, endpoint);
    defer allocator.free(endpoint_z);
    try req.connect(endpoint_z);

    try pingLoop(req, payload, secondsToNanos(warmup_s), null);

    var samples: std.array_list.Managed(f64) = .init(allocator);
    defer samples.deinit();
    try pingLoop(req, payload, secondsToNanos(duration_s), &samples);

    try req.sendSlice(stop, .{});
    var final: zimq.Message = .empty();
    _ = try req.recvMsg(&final, .{});
    final.deinit();
    thread.join();

    std.mem.sort(f64, samples.items, {}, comptime std.sort.asc(f64));
    try printFloat(init, percentile(samples.items, 50));
    std.process.exit(0);
}

const Sender = struct {
    socket: *zimq.Socket,
    payload: []const u8,
    duration_ns: u64,
};

fn sendLoop(sender: *Sender) !void {
    const start = nowNs();
    while (nowNs() - start < sender.duration_ns) {
        try sender.socket.sendSlice(sender.payload, .{});
    }
    try sender.socket.sendSlice(stop, .{});
}

const Echoer = struct {
    endpoint: []const u8,
    allocator: std.mem.Allocator,
};

fn echoLoop(echoer: *Echoer) !void {
    const ctx: *zimq.Context = try .init();
    defer ctx.deinit();
    const socket: *zimq.Socket = try .init(ctx, .rep);
    defer socket.deinit();
    const endpoint_z = try echoer.allocator.dupeZ(u8, echoer.endpoint);
    defer echoer.allocator.free(endpoint_z);
    try socket.bind(endpoint_z);

    while (true) {
        var msg: zimq.Message = .empty();
        _ = try socket.recvMsg(&msg, .{});
        const data = msg.slice();
        try socket.sendSlice(data, .{});
        const done = std.mem.eql(u8, data, stop);
        msg.deinit();
        if (done) break;
    }
}

fn pingLoop(
    req: *zimq.Socket,
    payload: []const u8,
    duration_ns: u64,
    samples: ?*std.array_list.Managed(f64),
) !void {
    const start = nowNs();
    while (nowNs() - start < duration_ns) {
        const t0 = nowNs();
        try req.sendSlice(payload, .{});
        var msg: zimq.Message = .empty();
        _ = try req.recvMsg(&msg, .{});
        msg.deinit();
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

fn lastEndpoint(allocator: std.mem.Allocator, socket: *zimq.Socket) ![:0]u8 {
    var buffer: [512:0]u8 = undefined;
    var endpoint: [:0]u8 = buffer[0..];
    try socket.get(.last_endpoint, &endpoint);
    const end = std.mem.indexOfScalar(u8, endpoint, 0) orelse endpoint.len;
    return allocator.dupeZ(u8, endpoint[0..end]);
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
    try stderr.print("usage: zimq_bench throughput SIZE SECONDS | throughput-push SIZE SECONDS | throughput-pull ENDPOINT SIZE | latency SIZE WARMUP_SECONDS SECONDS ENDPOINT\n", .{});
    try stderr.flush();
    return error.InvalidArgs;
}
