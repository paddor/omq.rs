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
            "[{s}] RSS start {} MiB, final {} MiB, peak {} MiB; FDs start {}, final {}, peak {}\n",
            .{
                label,
                baseline.rss_bytes / 1_048_576,
                final.rss_bytes / 1_048_576,
                peak_rss / 1_048_576,
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
        if (baseline.rss_bytes != 0) {
            const rss_growth = final.rss_bytes -| baseline.rss_bytes;
            const allowed = @max(baseline.rss_bytes, 10 * 1_048_576);
            if (rss_growth > allowed) return error.RssLeak;
        }
    }
};

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
        defer allocator.free(msg);
        _ = state.rep.send(msg, 0) catch {
            storeBool(state.failed, true);
            return;
        };
    }
}

test "req rep cycles track resources" {
    const duration_ns = soakDurationNs();
    var monitor = try ResourceMonitor.init(allocator);
    defer monitor.deinit();

    var ctx = try omq.Context.init();
    defer ctx.deinit();

    var rep = try ctx.socket(omq.REP);
    defer rep.deinit();
    var req = try ctx.socket(omq.REQ);
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
        defer allocator.free(reply);
        try testing.expectEqualStrings(text, reply);
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

    var push = try ctx.socket(omq.PUSH);
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
        var pull = try ctx.socket(omq.PULL);
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
            var pull = try ctx.socket(omq.PULL);
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
