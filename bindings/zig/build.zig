const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const omq_include_dir =
        b.option([]const u8, "omq-include-dir", "Path containing zmq.h") orelse
        "../../omq-libzmq/include";
    const omq_lib_dir =
        b.option([]const u8, "omq-lib-dir", "Path containing libomq_zmq") orelse
        "../../target/release";
    const test_filter = b.option([]const u8, "test-filter", "Only build matching tests");
    const test_filters: []const []const u8 = if (test_filter) |filter| &.{filter} else &.{};

    const omq = b.addModule("omq", .{
        .root_source_file = b.path("src/omq.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureOmqModule(b, omq, omq_include_dir, omq_lib_dir);

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/basic.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "omq", .module = omq },
            },
            .link_libc = true,
        }),
        .filters = test_filters,
    });

    const run_tests = b.addRunArtifact(tests);
    if (b.args) |args| {
        run_tests.addArgs(args);
    }
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);

    const parity_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/parity.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "omq", .module = omq },
            },
            .link_libc = true,
        }),
        .filters = test_filters,
    });
    const run_parity_tests = b.addRunArtifact(parity_tests);
    if (b.args) |args| {
        run_parity_tests.addArgs(args);
    }
    test_step.dependOn(&run_parity_tests.step);

    const soak_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/soak.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "omq", .module = omq },
            },
            .link_libc = true,
        }),
        .filters = test_filters,
    });
    const run_soak_tests = b.addRunArtifact(soak_tests);
    if (b.args) |args| {
        run_soak_tests.addArgs(args);
    }
    const soak_step = b.step("soak", "Run long-running soak tests");
    soak_step.dependOn(&run_soak_tests.step);

    const docs_module = b.createModule(.{
        .root_source_file = b.path("src/omq.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    configureOmqModule(b, docs_module, omq_include_dir, omq_lib_dir);

    const docs_object = b.addObject(.{
        .name = "omq-docs",
        .root_module = docs_module,
    });
    const install_docs = b.addInstallDirectory(.{
        .source_dir = docs_object.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate API documentation");
    docs_step.dependOn(&install_docs.step);

    const bench = b.addExecutable(.{
        .name = "omq-zig-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("scripts/bench/omq_bench.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "omq", .module = omq },
            },
            .link_libc = true,
        }),
    });
    b.installArtifact(bench);
}

fn configureOmqModule(
    b: *std.Build,
    mod: *std.Build.Module,
    include_dir: []const u8,
    lib_dir: []const u8,
) void {
    const include_path = hostOrBuildPath(b, include_dir);
    const lib_path = hostOrBuildPath(b, lib_dir);
    mod.addIncludePath(include_path);
    mod.addLibraryPath(lib_path);
    mod.addRPath(lib_path);
    mod.linkSystemLibrary("omq_zmq", .{
        .use_pkg_config = .no,
        .search_strategy = .paths_first,
    });
}

fn hostOrBuildPath(b: *std.Build, path: []const u8) std.Build.LazyPath {
    if (std.fs.path.isAbsolute(path)) {
        return .{ .cwd_relative = path };
    }
    return b.path(path);
}
