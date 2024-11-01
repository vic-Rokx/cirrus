const std = @import("std");
const Cirrus = @import("cirrus.zig");
const helpers = @import("utils/helpers.zig");
const posix = std.posix;
const Parser = @import("parser.zig");
const error_fns = @import("utils/error.zig");
const assert = error_fns.assert_cm;
const Command = @import("types.zig").Command;
const print = std.debug.print;
const Cache = @import("cache.zig");
const Entry = Cache.Entry;
const DLL = @import("storage/dll.zig").DLinkedList;
const Types = @import("types.zig");
const connections = @import("cache_runtime/connections.zig");
const worker = connections.worker;
const snapCaches = @import("snapshot.zig").snapCaches;
const loadCacheFromDisk = @import("snapshot.zig").loadCacheFromDisk;
const SnapShotError = @import("snapshot.zig").SnapShotError;
const SnapShot = @import("snapshot.zig");
const poller = @import("poller.zig").pollConnections;

const DEFAULT_PORT: usize = 6379;

pub const State = enum {
    RESP,
    REQ,
    Processing,
    End,
};

// For some reason we get an alias error when the buffer is set to 512
// this could be do to the internal syscall made by the kernal "posix"

pub const Config = struct {
    port: u16,
    addr: []const u8,
    arena: *std.mem.Allocator,
    replicas: u16,
};

pub const Conn = struct {
    fd: c_int,
    state: State,
    rbuf_size: usize,
    rbuf: [1024]u8,
    wbuf_size: usize,
    wbuf_sent: usize,
    wbuf: [1024]u8,
    buffer: []const u8,
    builder: *std.RingBuffer,
    start_time: i128,
    last_check: i128,
};

const Self = @This();
cache_count: u16,
replica_count: u16,
addresses: []const std.net.Address,
cache_configs: []const CacheConfig,
arena: *std.mem.Allocator,
caches: ?[]*Cache,
caches_inst: ?[]*Cirrus,
cluster_host: []const u8,
cluster_port: u16,
enable_snapshot: bool,
enable_multithread: bool,

const ClusterError = error{
    FailedToInitializeCache,
    FailedToInitializeCluster,
    FailedToAllocMemForCaches,
    FailedToRunCache,
};

pub const CacheConfig = struct {
    address: []const u8,
    port: u16,
};

pub const ClusterConfig = struct {
    cache_count: u16,
    replica_count: u16,
    cache_configs: []const CacheConfig,
    gpa: *std.mem.Allocator,
    cluster_host: []const u8,
    cluster_port: u16,
    enable_snapshot: bool,
    enable_multithread: bool,
};

pub fn init(target: *Self, config: ClusterConfig) !void {
    target.* = .{
        .cache_count = config.cache_count,
        .replica_count = config.replica_count,
        .addresses = undefined,
        .cache_configs = config.cache_configs,
        .arena = config.gpa,
        .caches = null,
        .caches_inst = null,
        .cluster_port = config.cluster_port,
        .cluster_host = config.cluster_host,
        .enable_snapshot = config.enable_snapshot,
        .enable_multithread = config.enable_multithread,
    };
}
pub fn deinit(self: *Self) !void {
    if (self.caches_inst == null) {
        return;
    }
    for (self.caches_inst.?) |cache_inst| {
        try cache_inst.*.deinit();
        self.arena.destroy(cache_inst); // Deallocate the cache
    }
    self.arena.free(self.caches_inst.?); // Deinitialize the ArrayList
    //
    if (self.caches == null) {
        return;
    }

    // for (self.caches.?) |cache| {
    // cache.*.deinit();
    // self.arena.destroy(cache); // Deallocate the cache
    // }
    // //
    self.arena.free(self.caches.?); // Deinitialize the ArrayList
}

pub fn createCluster(self: *Self) !void {
    helpers.assert_cm(self.cache_count > 0, "Cache count cannot be less than zero.");
    self.caches_inst = self.arena.*.alloc(*Cirrus, self.cache_count) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return ClusterError.FailedToAllocMemForCaches;
    };

    var cache_ptr: *Cirrus = undefined;
    for (self.cache_configs, 0..) |cache_config, i| {
        cache_ptr = try self.arena.*.create(Cirrus);
        const config = Cirrus.Config{
            .addr = cache_config.address,
            .port = cache_config.port,
            .arena = self.arena,
            .replicas = self.replica_count,
            .enabled_multithread = self.enable_multithread,
        };

        try cache_ptr.*.init(config);
        self.caches_inst.?[i] = cache_ptr;
    }
}

pub fn deinitCluster(self: *Self) !void {
    if (self.caches_inst == null) {
        return;
    }

    for (self.caches_inst.?) |cache_inst| {
        try cache_inst.*.deinit();
        self.arena.destroy(cache_inst); // Deallocate the cache
    }
    self.arena.free(self.caches_inst.?); // Deinitialize the ArrayList
}

fn runCache(cache_inst: *Cirrus) !void {
    try cache_inst.*.run();
}

pub fn populateCacheArray(self: *Self) !void {
    helpers.assert_cm(self.cache_count > 0, "Cache count cannot be less than zero.");
    self.caches = self.arena.*.alloc(*Cache, self.cache_count) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return ClusterError.FailedToAllocMemForCaches;
    };
    for (self.caches_inst.?, 0..) |cache_inst, i| {
        self.caches.?[i] = &cache_inst.cache;
    }
}

pub fn deinitCacheArray(self: *Self) !void {
    if (self.caches == null) {
        return;
    }

    // for (self.caches.?) |cache_inst| {
    //     try cache_inst.*.deinit();
    //     self.arena.destroy(cache_inst); // Deallocate the cache
    // }
    self.arena.free(self.caches.?); // Deinitialize the ArrayList
}

fn populateCacheData(self: *Self, snapshot: *SnapShot) !void {
    for (self.caches.?, 0..) |cache, i| {
        var buf_size: [256]u8 = undefined;
        const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_caches/snapped_cache_{}.dat", .{i});
        snapshot.loadCacheFromDisk(cacheName, cache) catch |err| {
            switch (err) {
                SnapShotError.FailedToLoadFile => {
                    helpers.error_print_str("\nFile does not exist");
                    continue;
                },
                SnapShotError.FailedToReadFile => {
                    helpers.error_print_str("\nCurrupted data");
                },
                else => {},
            }
        };

        cache.*.key_change_count = 0;
    }
}

pub fn run(self: *Self) !void {
    var poll_args = std.ArrayList(posix.pollfd).init(self.arena.*);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(self.arena.*);
    defer fd_conns.deinit();

    var threads: []*std.Thread = undefined;
    threads = self.arena.*.alloc(*std.Thread, self.cache_count) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return ClusterError.FailedToAllocMemForCaches;
    };
    defer self.arena.*.free(threads);

    const ip_addr = try std.net.Address.parseIp4(self.cluster_host, self.cluster_port);
    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    // print("Socket fd: {d}\n", .{socket_fd});
    defer posix.close(socket_fd);
    var option_value: i32 = 1; // Enable the option
    const option_value_bytes = std.mem.asBytes(&option_value);
    try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);

    try posix.bind(socket_fd, &ip_addr.any, ip_addr.getOsSockLen());

    // const reset = "\x1b[0m"; // ANSI escape code to reset color
    // const ascii_art =
    //     \\ ______     __     ______     ______     __  __     ______
    //     \\/\  ___\   /\ \   /\  == \   /\  == \   /\ \/\ \   /\  ___\
    //     \\\ \ \____  \ \ \  \ \  __<   \ \  __<   \ \ \_\ \  \ \___  \
    //     \\ \ \_____\  \ \_\  \ \_\ \_\  \ \_\ \_\  \ \_____\  \/\_____\
    //     \\  \/_____/   \/_/   \/_/ /_/   \/_/ /_/   \/_____/   \/_____/
    // ;
    // print("\n{s}{s}\n", .{ ascii_art, reset });

    std.debug.print("Initializing Cirrus Cluster size {d}\n", .{self.cache_count});
    try self.createCluster();
    defer (self.deinitCluster()) catch @panic("Could not deinit cluster");
    try self.populateCacheArray();
    defer (self.deinitCacheArray()) catch @panic("Could not deinit Cache array");

    helpers.assert_cm(self.caches_inst != null, "Cache Instances are not initilized,\nPlease run createCluster...");
    helpers.assert_cm(self.caches != null, "Caches are not initilized,\nPlease run createCluster...");

    for (self.caches_inst.?, 0..) |cache, i| {
        // _ = try std.Thread.spawn(.{}, runCache, .{self.caches_inst.?[0]});
        var cache_thread = try std.Thread.spawn(.{}, runCache, .{cache});
        threads[i] = &cache_thread;
        // defer cache_thread.join();
    }

    // Initialize SnapShot
    // if (self.enable_snapshot) {
    //     var snapshot: SnapShot = undefined;
    //     snapshot.init(self.arena);
    //     try self.populateCacheData(&snapshot);
    //     _ = try std.Thread.spawn(.{}, snapCaches, .{&self.caches.?});
    //     // defer snap_thread.join();
    // }

    try posix.listen(socket_fd, 128);
    std.debug.print("Cirrus Cluster listening on {}\n", .{ip_addr});

    try poller(socket_fd, &poll_args, &fd_conns, &self.caches.?, self.arena);
}

test "test memory leaks cluster init" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();

    const cache_configs = [_]CacheConfig{
        CacheConfig{
            .port = 7000,
            .address = "127.0.0.1",
        },
        CacheConfig{
            .port = 7001,
            .address = "127.0.0.1",
        },
        CacheConfig{
            .port = 7002,
            .address = "127.0.0.1",
        },
    };

    const config_cluster = ClusterConfig{
        .cache_count = cache_configs.len,
        .replica_count = 2,
        .cache_configs = &cache_configs,
        .gpa = &arena,
        .cluster_host = "127.0.0.1",
        .cluster_port = 6379,
        .enable_snapshot = true,
        .enable_multithread = false,
    };
    var cluster: Self = undefined;
    try cluster.init(config_cluster);

    const ip_addr = try std.net.Address.parseIp4(cluster.cluster_host, cluster.cluster_port);
    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    // print("Socket fd: {d}\n", .{socket_fd});
    defer posix.close(socket_fd);

    var poll_args = std.ArrayList(posix.pollfd).init(arena);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(arena);
    defer fd_conns.deinit();

    var threads: []*std.Thread = undefined;
    threads = arena.alloc(*std.Thread, 3) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return ClusterError.FailedToAllocMemForCaches;
    };

    defer arena.free(threads);
    try cluster.createCluster();
    defer (cluster.deinitCluster()) catch @panic("Could not deinit cluster");
    try cluster.populateCacheArray();
    defer (cluster.deinitCacheArray()) catch @panic("Could not deinit Cache array");

    for (cluster.caches_inst.?, 0..) |cache, i| {
        var cache_thread = try std.Thread.spawn(.{}, runCache, .{cache});
        threads[i] = &cache_thread;
        defer cache_thread.join();
    }

    try posix.listen(socket_fd, 128);
    std.debug.print("Cirrus Cluster listening on {}\n", .{ip_addr});

    // try poller(socket_fd, &poll_args, &fd_conns, &cluster.caches.?, cluster.arena);
}
