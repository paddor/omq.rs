const std = @import("std");
const builtin = @import("builtin");
const omq = @import("omq");

const c = @cImport({
    @cInclude("dirent.h");
    @cInclude("stdio.h");
    @cInclude("stdlib.h");
    @cInclude("time.h");
});

const testing = std.testing;
const allocator = std.heap.page_allocator;
const ns_per_s = std.time.ns_per_s;
const default_duration_ns: i128 = 120 * ns_per_s;
const mib = 1_048_576;
const large_message_size = 1024 * 1024;

const Sample = struct {
    rss_bytes: u64,
    fd_count: u64,
};

const ResourceMonitor = struct {
    samples: std.array_list.Managed(Sample),

    fn init(alloc: std.mem.Allocator) !ResourceMonitor {
        var monitor: ResourceMonitor = .{ .samples = .init(alloc) };
        try monitor.sample();
        return monitor;
    }

    fn deinit(self: *ResourceMonitor) void {
        self.samples.deinit();
    }

    fn sample(self: *ResourceMonitor) !void {
        try self.samples.append(.{
            .rss_bytes = readRssBytes(),
            .fd_count = readFdCount(),
        });
    }

    fn assertNoLeak(self: *ResourceMonitor, label: []const u8, peak_fd_slack: u64) !void {
        try self.sample();
        const samples = self.samples.items;
        if (samples.len == 0) return;

        const baseline = samples[0];
        const final = samples[samples.len - 1];
        var peak_rss: u64 = 0;
        var peak_fds: u64 = 0;
        for (samples) |entry| {
            peak_rss = @max(peak_rss, entry.rss_bytes);
            peak_fds = @max(peak_fds, entry.fd_count);
        }

        std.debug.print(
            "[{s}] resources: RSS start {} MiB, final {} MiB, peak {} MiB; FDs start {}, final {}, peak {}\n",
            .{
                label,
                baseline.rss_bytes / mib,
                final.rss_bytes / mib,
                peak_rss / mib,
                baseline.fd_count,
                final.fd_count,
                peak_fds,
            },
        );

        if (baseline.fd_count != 0 and final.fd_count > baseline.fd_count + 4) {
            return error.FdLeak;
        }
        if (baseline.fd_count != 0 and peak_fds > baseline.fd_count + peak_fd_slack) {
            return error.FdLeak;
        }

        if (samples.len < 10) {
            std.debug.print("[{s}] too few RSS samples ({}) to check for leaks\n", .{ label, samples.len });
            return;
        }

        const warmup = samples.len / 5;
        const post_warmup = samples[warmup..];
        if (post_warmup.len == 0) return;

        const baseline_count = @max(post_warmup.len / 10, 1);
        var baseline_sum: u128 = 0;
        for (post_warmup[0..baseline_count]) |entry| {
            baseline_sum += entry.rss_bytes;
        }
        const rss_baseline: u64 = @intCast(baseline_sum / baseline_count);

        const tail_start = post_warmup.len * 4 / 5;
        var tail_max: u64 = 0;
        for (post_warmup[tail_start..]) |entry| {
            tail_max = @max(tail_max, entry.rss_bytes);
        }

        const tail_growth = tail_max -| rss_baseline;
        const final_growth = final.rss_bytes -| rss_baseline;
        const final_growth_mib = bytesToMib(final_growth);
        const tail_growth_pct = percentGrowth(tail_growth, rss_baseline);
        const final_growth_pct = percentGrowth(final_growth, rss_baseline);

        std.debug.print(
            "[{s}] RSS: baseline {d:.1} MiB, tail max {d:.1} MiB, final {d:.1} MiB, peak {d:.1} MiB, tail growth {d:.1}%, final growth {d:.1}%\n",
            .{
                label,
                bytesToMib(rss_baseline),
                bytesToMib(tail_max),
                bytesToMib(final.rss_bytes),
                bytesToMib(peak_rss),
                tail_growth_pct,
                final_growth_pct,
            },
        );

        const threshold_pct: f64 = if (samples.len >= 120) 25.0 else 100.0;
        if (final_growth_pct >= threshold_pct and final_growth_mib >= 10.0) return error.RssLeak;
    }
};

fn bytesToMib(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / mib;
}

fn percentGrowth(growth: u64, baseline: u64) f64 {
    if (baseline == 0) return 0.0;
    return @as(f64, @floatFromInt(growth)) / @as(f64, @floatFromInt(baseline)) * 100.0;
}

fn readRssBytes() u64 {
    if (builtin.os.tag != .linux) return 0;
    const file = c.fopen("/proc/self/statm", "r") orelse return 0;
    defer _ = c.fclose(file);

    var buf = std.mem.zeroes([128]u8);
    _ = c.fgets(&buf, buf.len, file) orelse return 0;
    var it = std.mem.tokenizeScalar(u8, std.mem.sliceTo(&buf, 0), ' ');
    _ = it.next() orelse return 0;
    const resident = it.next() orelse return 0;
    const pages = std.fmt.parseUnsigned(u64, resident, 10) catch return 0;
    return pages * 4096;
}

fn readFdCount() u64 {
    if (builtin.os.tag != .linux) return 0;
    const dir = c.opendir("/proc/self/fd") orelse return 0;
    defer _ = c.closedir(dir);

    var count: u64 = 0;
    while (c.readdir(dir) != null) {
        count += 1;
    }
    return count;
}

fn soakDurationNs() i128 {
    const raw = c.getenv("OMQ_ZIG_SOAK_DURATION_SECS") orelse return default_duration_ns;
    const seconds = std.fmt.parseFloat(f64, std.mem.span(raw)) catch return default_duration_ns;
    return @intFromFloat(seconds * @as(f64, @floatFromInt(ns_per_s)));
}

fn nowNs() i128 {
    var ts: c.struct_timespec = undefined;
    if (c.clock_gettime(c.CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (@as(i128, ts.tv_sec) * ns_per_s) + ts.tv_nsec;
}

fn sleepMillis(ms: u64) void {
    const request: std.c.timespec = .{
        .sec = @intCast(ms / 1000),
        .nsec = @intCast((ms % 1000) * std.time.ns_per_ms),
    };
    _ = std.c.nanosleep(&request, null);
}

fn loadBool(ptr: *bool) bool {
    return @atomicLoad(bool, ptr, .acquire);
}

fn storeBool(ptr: *bool, value: bool) void {
    @atomicStore(bool, ptr, value, .release);
}

fn addCounter(ptr: *u64, value: u64) void {
    _ = @atomicRmw(u64, ptr, .Add, value, .monotonic);
}

fn loadCounter(ptr: *u64) u64 {
    return @atomicLoad(u64, ptr, .monotonic);
}

fn soakSocket(ctx: *omq.Context, socket_type: i32) !omq.Socket {
    var socket = try ctx.socket(socket_type);
    errdefer socket.deinit();
    try socket.setLinger(0);
    return socket;
}

const ReqRepServer = struct {
    rep: *omq.Socket,
    stop: *bool,
    failed: *bool,
};

fn reqRepServer(state: *ReqRepServer) void {
    while (!loadBool(state.stop)) {
        const msg = state.rep.recvAlloc(allocator, 0) catch |err| switch (err) {
            error.Again => continue,
            else => {
                storeBool(state.failed, true);
                return;
            },
        };
        _ = state.rep.send(msg, 0) catch {
            allocator.free(msg);
            storeBool(state.failed, true);
            return;
        };
        allocator.free(msg);
    }
}

test "context churn tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var iterations: u64 = 0;

    while (nowNs() - start < duration_ns) {
        var ctx = try omq.Context.init();

        var pull = try soakSocket(&ctx, omq.PULL);
        errdefer pull.deinit();
        var push = try soakSocket(&ctx, omq.PUSH);
        errdefer push.deinit();

        try pull.setReceiveTimeout(1000);
        try push.setSendTimeout(1000);

        var endpoint_buf: [96]u8 = undefined;
        const endpoint = try std.fmt.bufPrint(&endpoint_buf, "inproc://zig-soak-context-{d}", .{iterations});
        const bound = try pull.bind(allocator, endpoint);
        try push.connect(allocator, bound);

        _ = try push.send("churn", 0);
        const got = try pull.recvAlloc(allocator, 0);
        try testing.expectEqualStrings("churn", got);
        allocator.free(got);

        try push.close();
        try pull.close();
        try ctx.term();
        allocator.free(bound);
        iterations += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[context_churn] {} contexts\n", .{iterations});
        }
    }

    try testing.expect(iterations > 0);
    try monitor.assertNoLeak("context_churn", 16);
}

test "req rep cycles track resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try soakSocket(&ctx, omq.REP);
    defer rep.deinit();
    var req = try soakSocket(&ctx, omq.REQ);
    defer req.deinit();

    try rep.setReceiveTimeout(100);
    try rep.setSendTimeout(5000);
    try req.setReceiveTimeout(5000);
    try req.setSendTimeout(5000);

    const bound = try rep.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try req.connect(allocator, bound);

    var stop = false;
    var failed = false;
    var server_state: ReqRepServer = .{ .rep = &rep, .stop = &stop, .failed = &failed };
    const thread = try std.Thread.spawn(.{}, reqRepServer, .{&server_state});

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var cycles: u64 = 0;
    var payload: [32]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const text = try std.fmt.bufPrint(&payload, "r-{d}", .{cycles});
        _ = try req.send(text, 0);
        const reply = try req.recvAlloc(allocator, 0);
        try testing.expectEqualStrings(text, reply);
        allocator.free(reply);
        cycles += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[req_rep] {} cycles\n", .{cycles});
        }
        try testing.expect(!loadBool(&failed));
    }

    storeBool(&stop, true);
    thread.join();
    try testing.expect(!loadBool(&failed));
    try testing.expect(cycles > 0);

    try req.close();
    try rep.close();
    try ctx.term();
    try monitor.assertNoLeak("req_rep", 32);
}

test "push pull sustained tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try soakSocket(&ctx, omq.PULL);
    defer pull.deinit();
    var push = try soakSocket(&ctx, omq.PUSH);
    defer push.deinit();

    try pull.setReceiveTimeout(1000);
    try push.setSendTimeout(1000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var messages: u64 = 0;
    var payload: [32]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const text = try std.fmt.bufPrint(&payload, "p-{d}", .{messages});
        _ = try push.send(text, 0);
        const got = try pull.recvAlloc(allocator, 0);
        try testing.expectEqualStrings(text, got);
        allocator.free(got);
        messages += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[push_pull] {} messages\n", .{messages});
        }
    }

    try push.close();
    try pull.close();
    try ctx.term();

    try testing.expect(messages > 0);
    try monitor.assertNoLeak("push_pull", 32);
}

test "pair bidirectional tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var a = try soakSocket(&ctx, omq.PAIR);
    defer a.deinit();
    var b = try soakSocket(&ctx, omq.PAIR);
    defer b.deinit();

    try a.setReceiveTimeout(1000);
    try b.setReceiveTimeout(1000);
    try a.setSendTimeout(1000);
    try b.setSendTimeout(1000);

    const bound = try a.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try b.connect(allocator, bound);

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var cycles: u64 = 0;
    var payload: [32]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const text = try std.fmt.bufPrint(&payload, "pair-{d}", .{cycles});
        _ = try a.send(text, 0);
        const left = try b.recvAlloc(allocator, 0);
        try testing.expectEqualStrings(text, left);
        allocator.free(left);

        _ = try b.send(text, 0);
        const right = try a.recvAlloc(allocator, 0);
        try testing.expectEqualStrings(text, right);
        allocator.free(right);
        cycles += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[pair] {} cycles\n", .{cycles});
        }
    }

    try b.close();
    try a.close();
    try ctx.term();

    try testing.expect(cycles > 0);
    try monitor.assertNoLeak("pair", 32);
}

test "multipart push pull tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try soakSocket(&ctx, omq.PULL);
    defer pull.deinit();
    var push = try soakSocket(&ctx, omq.PUSH);
    defer push.deinit();

    try pull.setReceiveTimeout(1000);
    try push.setSendTimeout(1000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var messages: u64 = 0;
    var tail_buf: [32]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const tail = try std.fmt.bufPrint(&tail_buf, "tail-{d}", .{messages});
        try push.sendMultipart(&.{ "head", "body", tail }, 0);
        var got = try pull.recvMultipartAlloc(allocator, 0);
        try testing.expectEqual(@as(usize, 3), got.parts.len);
        try testing.expectEqualStrings("head", got.parts[0]);
        try testing.expectEqualStrings("body", got.parts[1]);
        try testing.expectEqualStrings(tail, got.parts[2]);
        got.deinit();
        messages += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[multipart] {} messages\n", .{messages});
        }
    }

    try push.close();
    try pull.close();
    try ctx.term();

    try testing.expect(messages > 0);
    try monitor.assertNoLeak("multipart", 32);
}

test "pub sub sustained tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var publisher = try soakSocket(&ctx, omq.PUB);
    defer publisher.deinit();
    var sub = try soakSocket(&ctx, omq.SUB);
    defer sub.deinit();

    try sub.subscribe("topic/");
    try sub.setReceiveTimeout(100);

    const bound = try publisher.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try sub.connect(allocator, bound);
    sleepMillis(300);

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var sent: u64 = 0;
    var received: u64 = 0;
    var payload: [48]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const text = try std.fmt.bufPrint(&payload, "topic/{d}", .{sent});
        _ = try publisher.send(text, 0);
        sent += 1;

        const got = sub.recvAlloc(allocator, 0) catch |err| switch (err) {
            error.Again => null,
            else => return err,
        };
        if (got) |msg| {
            allocator.free(msg);
            received += 1;
        }

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[pub_sub] sent {}, received {}\n", .{ sent, received });
        }
    }

    try sub.close();
    try publisher.close();
    try ctx.term();

    try testing.expect(sent > 0);
    try testing.expect(received > 0);
    try monitor.assertNoLeak("pub_sub", 32);
}

test "large messages track resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var pull = try soakSocket(&ctx, omq.PULL);
    defer pull.deinit();
    var push = try soakSocket(&ctx, omq.PUSH);
    defer push.deinit();

    try pull.setReceiveHighWaterMark(4);
    try push.setSendHighWaterMark(4);
    try pull.setReceiveTimeout(5000);
    try push.setSendTimeout(5000);

    const bound = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);
    try push.connect(allocator, bound);

    const payload = try allocator.alloc(u8, large_message_size);
    defer allocator.free(payload);
    for (payload, 0..) |*byte, index| {
        byte.* = @intCast(index & 0xff);
    }

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var messages: u64 = 0;
    while (nowNs() - start < duration_ns) {
        _ = try push.send(payload, 0);
        const got = try pull.recvAlloc(allocator, 0);
        try testing.expectEqual(@as(usize, large_message_size), got.len);
        try testing.expectEqual(payload[0], got[0]);
        try testing.expectEqual(payload[payload.len - 1], got[got.len - 1]);
        allocator.free(got);
        messages += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[large_msg] {} messages\n", .{messages});
        }
    }

    try push.close();
    try pull.close();
    try ctx.term();

    try testing.expect(messages > 0);
    try monitor.assertNoLeak("large_msg", 32);
}

fn bindReconnectPull(ctx: *omq.Context, endpoint: []const u8) !omq.Socket {
    var pull = try soakSocket(ctx, omq.PULL);
    errdefer pull.deinit();
    try pull.setReceiveTimeout(500);

    for (0..40) |_| {
        const bound = pull.bind(allocator, endpoint) catch |err| switch (err) {
            error.AddressInUse => {
                sleepMillis(25);
                continue;
            },
            else => return err,
        };
        allocator.free(bound);
        return pull;
    }
    return error.AddressInUse;
}

test "reconnect storm tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try soakSocket(&ctx, omq.PUSH);
    defer push.deinit();
    try push.setSendTimeout(500);
    try push.setReconnectInterval(10);

    var pull = try soakSocket(&ctx, omq.PULL);
    try pull.setReceiveTimeout(500);
    const endpoint = try pull.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(endpoint);
    try push.connect(allocator, endpoint);

    const start = nowNs();
    var next_sample = start + ns_per_s;
    var cycles: u64 = 0;
    var delivered: u64 = 0;
    var payload: [32]u8 = undefined;
    while (nowNs() - start < duration_ns) {
        const text = try std.fmt.bufPrint(&payload, "c-{d}", .{cycles});
        _ = push.send(text, 0) catch |err| switch (err) {
            error.Again => {},
            else => return err,
        };

        const got = pull.recvAlloc(allocator, 0) catch |err| switch (err) {
            error.Again => null,
            else => return err,
        };
        if (got) |msg| {
            try testing.expectEqualStrings(text, msg);
            allocator.free(msg);
            delivered += 1;
        }

        try pull.close();
        pull = try bindReconnectPull(&ctx, endpoint);
        cycles += 1;

        const now = nowNs();
        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print("[reconnect_storm] cycles {}, delivered {}\n", .{ cycles, delivered });
        }
    }

    try pull.close();
    try push.close();
    try ctx.term();

    try testing.expect(cycles > 0);
    try monitor.assertNoLeak("reconnect_storm", 32);
}

const Peer = struct {
    socket: omq.Socket,
    connected: bool,
};

test "peer churn tracks resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var push = try soakSocket(&ctx, omq.PUSH);
    defer push.deinit();
    try push.setSendTimeout(1);
    try push.setSendHighWaterMark(1024);
    const bound = try push.bind(allocator, "tcp://127.0.0.1:*");
    defer allocator.free(bound);

    var peers: std.array_list.Managed(Peer) = .init(allocator);
    defer {
        for (peers.items) |*peer| peer.socket.deinit();
        peers.deinit();
    }

    for (0..20) |_| {
        var pull = try soakSocket(&ctx, omq.PULL);
        try pull.setReceiveTimeout(0);
        try pull.connect(allocator, bound);
        try peers.append(.{ .socket = pull, .connected = true });
    }
    sleepMillis(100);

    const start = nowNs();
    var next_tick = start;
    var next_sample = start + ns_per_s;
    var ticks: u64 = 0;
    var sent: u64 = 0;
    var partitions: u64 = 0;
    var heals: u64 = 0;
    var replaced: u64 = 0;
    var buf: [64]u8 = undefined;

    while (nowNs() - start < duration_ns) {
        const now = nowNs();
        if (now < next_tick) {
            sleepMillis(1);
            continue;
        }
        next_tick = now + 100 * std.time.ns_per_ms;
        ticks += 1;

        if (ticks % 19 == 0) {
            const index = ticks % peers.items.len;
            peers.items[index].socket.deinit();
            var pull = try soakSocket(&ctx, omq.PULL);
            try pull.setReceiveTimeout(0);
            try pull.connect(allocator, bound);
            peers.items[index] = .{ .socket = pull, .connected = true };
            replaced += 1;
        } else if (ticks % 7 == 0) {
            const index = ticks % peers.items.len;
            if (peers.items[index].connected) {
                try peers.items[index].socket.disconnect(allocator, bound);
                peers.items[index].connected = false;
                partitions += 1;
            } else {
                try peers.items[index].socket.connect(allocator, bound);
                peers.items[index].connected = true;
                heals += 1;
            }
        }

        for (0..100) |_| {
            const msg = try std.fmt.bufPrint(&buf, "soak-{d}", .{sent});
            _ = push.send(msg, 0) catch |err| switch (err) {
                error.Again => break,
                else => return err,
            };
            sent += 1;
        }

        for (peers.items) |*peer| {
            if (!peer.connected) continue;
            while (true) {
                var recv_buf: [64]u8 = undefined;
                _ = peer.socket.recvInto(&recv_buf, 0) catch |err| switch (err) {
                    error.Again => break,
                    else => return err,
                };
            }
        }

        if (now >= next_sample) {
            try monitor.sample();
            next_sample = now + ns_per_s;
            std.debug.print(
                "[peer_churn] sent {}, partitions {}, heals {}, replaced {}\n",
                .{ sent, partitions, heals, replaced },
            );
        }
    }

    for (peers.items) |*peer| {
        try peer.socket.close();
    }
    peers.clearRetainingCapacity();
    try push.close();
    try ctx.term();

    try testing.expect(sent > 0);
    try monitor.assertNoLeak("peer_churn", 128);
}
