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
const parseCommand = connections.parseCommand;
const snapCaches = @import("snapshot.zig").snapCaches;
const loadCacheFromDisk = @import("snapshot.zig").loadCacheFromDisk;
const SnapShotError = @import("snapshot.zig").SnapShotError;
const SnapShot = @import("snapshot.zig");

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
    };
}
pub fn deinit(self: *Self) !void {
    if (self.caches_inst == null) {
        return;
    }
    for (self.caches_inst.?) |cache_inst| {
        try cache_inst.deinit();
        self.arena.destroy(cache_inst); // Deallocate the cache
    }

    self.arena.free(self.caches.?); // Deinitialize the ArrayList
    self.arena.free(self.caches_inst.?); // Deinitialize the ArrayList
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
        };

        try cache_ptr.*.init(config);
        self.caches_inst.?[i] = cache_ptr;
    }
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

fn populateCacheData(self: *Self, snapshot: *SnapShot) !void {
    for (self.caches.?, 0..) |cache, i| {
        var buf_size: [256]u8 = undefined;
        const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_cache_{}.dat", .{i});
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
    const ip_addr = try std.net.Address.parseIp4(self.cluster_host, self.cluster_port);
    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    // print("Socket fd: {d}\n", .{socket_fd});
    defer posix.close(socket_fd);
    var option_value: i32 = 1; // Enable the option
    const option_value_bytes = std.mem.asBytes(&option_value);
    try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);

    try posix.bind(socket_fd, &ip_addr.any, ip_addr.getOsSockLen());
    try posix.listen(socket_fd, 128);

    std.debug.print("Running Cirrus Cluster on {}\n", .{ip_addr});

    // const reset = "\x1b[0m"; // ANSI escape code to reset color
    // const ascii_art =
    //     \\ ______     __     ______     ______     __  __     ______
    //     \\/\  ___\   /\ \   /\  == \   /\  == \   /\ \/\ \   /\  ___\
    //     \\\ \ \____  \ \ \  \ \  __<   \ \  __<   \ \ \_\ \  \ \___  \
    //     \\ \ \_____\  \ \_\  \ \_\ \_\  \ \_\ \_\  \ \_____\  \/\_____\
    //     \\  \/_____/   \/_/   \/_/ /_/   \/_/ /_/   \/_____/   \/_____/
    // ;
    // print("\n{s}{s}\n", .{ ascii_art, reset });

    var snapshot: SnapShot = undefined;
    snapshot.init(self.arena);
    try self.createCluster();
    try self.populateCacheArray();
    try self.populateCacheData(&snapshot);

    var threads: []*std.Thread = undefined;
    helpers.assert_cm(self.caches_inst != null, "Cache Instances are not initilized,\nPlease run createCluster...");
    helpers.assert_cm(self.caches != null, "Caches are not initilized,\nPlease run createCluster...");
    threads = self.arena.*.alloc(*std.Thread, 3) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return ClusterError.FailedToAllocMemForCaches;
    };
    for (self.caches_inst.?, 0..) |cache, i| {
        var thread = try std.Thread.spawn(.{}, runCache, .{cache});
        threads[i] = &thread;
    }
    // Initialize SnapShot
    // _ = try std.Thread.spawn(.{}, snapCaches, .{&self.caches.?});

    var poll_args = std.ArrayList(posix.pollfd).init(self.arena.*);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(self.arena.*);
    defer fd_conns.deinit();

    while (true) {
        poll_args.clearRetainingCapacity();
        const fd: posix.pollfd = .{
            .fd = socket_fd,
            .events = posix.POLL.IN,
            .revents = 0,
        };
        try poll_args.append(fd);
        assert(poll_args.items.len > 0, "no connections");
        for (fd_conns.items) |conn| {
            var events: i16 = posix.POLL.OUT;
            if (conn.state == State.REQ) {
                events = posix.POLL.IN;
            }

            assert(conn.fd > 1, "File descriptor is less than 0");
            var pfd: posix.pollfd = .{
                .fd = conn.fd,
                .events = events,
                .revents = 0,
            };

            pfd.events = pfd.events | posix.POLL.ERR;

            try poll_args.append(pfd);
        }

        const rv = try posix.poll(poll_args.items, 1000);

        if (rv < 0) {
            return error.PollingFailed;
        }

        if (poll_args.items.len > 0) {
            for (poll_args.items, 0..) |item, i| {
                if (i == 0) {
                    continue;
                }
                if (item.revents & posix.POLL.IN != 0 or item.revents & posix.POLL.OUT != 0) {
                    print("\nClient fd, {d} {d}", .{ std.time.nanoTimestamp(), self.cluster_port });
                    if (i - 1 >= fd_conns.items.len) {
                        return;
                    }
                    const conn = fd_conns.items[i - 1];
                    print("\nClient fd, {d}", .{conn.fd});
                    // std.time.sleep(1_000_000_000);

                    conn.*.start_time = std.time.nanoTimestamp();

                    assert(conn.fd > 0, "No valid connection");
                    connectionIo(conn, &self.caches.?, self.arena) catch |err| {
                        print("\nState machine event error: {any}", .{err});
                        return error.ReadFailed;
                    };
                }
            }
        }

        // Here we loop the responses
        for (fd_conns.items, 0..) |_, i| {
            var conn = fd_conns.items[i].*;

            if (conn.state == State.Processing) {
                try connectionIo(&conn, &self.caches.?, self.arena);
            }

            if (conn.state == State.RESP) {
                try connectionIo(&conn, &self.caches.?, self.arena);
            }

            if (conn.state == State.End) {
                assert(conn.fd > 0, "Not valid connection fd");
                print("\nClosing connection: {d}", .{conn.fd});
                posix.close(conn.fd);
                _ = fd_conns.swapRemove(i);
            }
        }

        if (poll_args.items[0].revents == 1) {
            print("\nRecieved a new connection", .{});
            try connections.acceptNewConnection(&fd_conns, socket_fd);
        }
    }
}

fn hashKey(key: []const u8) u32 {
    var h: u32 = 5381;
    for (key) |char| {
        h = ((h << 5) +% h) +% char;
    }
    h = h *% 2654435761;

    return h;
}

fn parseCaches(conn: *Conn, caches: *const []*Cache, arena: *std.mem.Allocator) !void {
    const command = try parseCommand(conn, arena);
    if (command) |cmd| {
        switch (cmd) {
            .ping => {
                try worker(cmd, conn, caches.*[0], arena);
            },
            .echo => |v| {
                const hash = hashKey(v);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .set => |v| {
                const hash = hashKey(v.key);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .get => |v| {
                const hash = hashKey(v.key);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .lpush => |v| {
                const hash = hashKey(v.dll_name);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .lpushmany => |v| {
                const hash = hashKey(v.dll_name);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .lrange => |v| {
                const hash = hashKey(v.dll_name);
                const idx = hash % caches.len;
                try worker(cmd, conn, caches.*[idx], arena);
            },
        }
    }

    conn.state = State.RESP;
    return;
}

fn connectionIo(conn: *Conn, caches: *const []*Cache, arena: *std.mem.Allocator) !void {
    switch (conn.state) {
        State.REQ => {
            print("\nreq state\n", .{});
            try connections.stateReq(conn);
        },
        State.Processing => {
            parseCaches(conn, caches, arena) catch |err| {
                print("\nWorker event error: {any}", .{err});
                return error.ReadFailed;
            };
        },
        State.RESP => {
            try connections.stateResp(conn);
        },
        State.End => {},
    }
}
