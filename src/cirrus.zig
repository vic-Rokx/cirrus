const std = @import("std");
const posix = std.posix;
const Parser = @import("parser.zig");
const error_fns = @import("utils/error.zig");
const Command = @import("types.zig").Command;
const print = std.debug.print;
const Cache = @import("cache.zig");
const DLL = @import("storage/dll.zig").DLinkedList;
const Types = @import("types.zig");
const assert = error_fns.assert_cm;
const connections = @import("cache_runtime/connections.zig");
const Conn = @import("cluster.zig").Conn;
const State = @import("cluster.zig").State;
const worker = connections.worker;
const poller = @import("poller.zig").pollConnections;

const DEFAULT_PORT: usize = 6379;

pub const CacheHealth = enum {
    Alive,
    Down,
    Crashed,
    Waiting,
    Ready,
};

const Self = @This();
socket_fd: posix.socket_t,
address: []const u8,
port: u16,
arena: *std.mem.Allocator,
addr: std.net.Address,
replicas: u16,
cache: Cache,
enabled_multithread: bool,
health: CacheHealth,
caches: []*Cache,

// For some reason we get an alias error when the buffer is set to 512
// this could be do to the internal syscall made by the kernal "posix"

pub const Config = struct {
    port: u16,
    addr: []const u8,
    arena: *std.mem.Allocator,
    replicas: u16,
    enabled_multithread: bool,
};

const CacheError = error{
    FailedToAllocMemForSingleCache,
    CacheCrashed,
};

pub fn init(target: *Self, config: Config) !void {
    const ip_addr = try std.net.Address.parseIp4(config.addr, config.port);
    var cache: Cache = undefined;
    try cache.init(config.arena, config.replicas, config.enabled_multithread);
    target.* = .{
        .socket_fd = undefined,
        .address = config.addr,
        .port = config.port,
        .arena = config.arena,
        .addr = ip_addr,
        .replicas = config.replicas,
        .cache = cache,
        .enabled_multithread = config.enabled_multithread,
        .health = CacheHealth.Ready,
        .caches = undefined,
    };
}

pub fn deinit(self: *Self) !void {
    self.cache.deinit();
}

fn healthCheck(self: *Self) !void {
    while (true) {
        std.time.sleep(5_000_000_000);
        const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        var option_value: i32 = 1; // Enable the option
        const option_value_bytes = std.mem.asBytes(&option_value);
        try posix.setsockopt(client_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);
        posix.connect(client_fd, &self.addr.any, self.addr.getOsSockLen()) catch |err| {
            return err;
        };

        // const rng = std.crypto.random;
        // const value = rng.int(u32);
        // if (value % 5 == 0) { // Simulate a failure 20% of the time.
        //     // std.debug.print("\nRunning ping to cache Failed", .{});
        //     // std.debug.print("\nServer connection refused {}\n", .{self.addr});
        //     // self.health = CacheHealth.Crashed;
        //     // return CacheError.CacheCrashed;
        // }
        const nw = try posix.write(client_fd, "$4\r\nPING\r\n");
        if (nw < 0) {
            return CacheError.CacheCrashed;
        }
        std.debug.print("\nPing successful", .{});
        posix.close(client_fd);
    }
}

pub fn run(self: *Self) !void {
    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    // self.socket_fd = socket_fd;
    defer posix.close(socket_fd);

    var option_value: i32 = 1; // Enable the option
    const option_value_bytes = std.mem.asBytes(&option_value);
    try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);

    // var adder_in: posix.sockaddr.in = undefined;
    // adder_in.family = posix.AF.INET;
    // adder_in.port = DEFAULT_PORT;
    // adder_in.addr = 0x7F000001;
    // const generic_addr: *const posix.sockaddr = @ptrCast(&adder_in);

    try posix.bind(socket_fd, &self.addr.any, self.addr.getOsSockLen());
    try posix.listen(socket_fd, 128);

    print("Running Nimbus Cache on {s}:{d}...\n", .{ self.address, self.port });
    self.health = CacheHealth.Alive;

    var poll_args = std.ArrayList(posix.pollfd).init(self.arena.*);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(self.arena.*);
    defer fd_conns.deinit();
    // self.caches = self.arena.*.alloc(*Cache, 1) catch {
    //     std.debug.print("\nFailed to alloc memory for caches", .{});
    //     return CacheError.FailedToAllocMemForSingleCache;
    // };
    // var caches = [_]*Cache{&self.cache};
    // caches[0] = &self.cache;
    // self.caches = &caches;

    var caches = [_]*Cache{&self.cache};
    caches[0] = &self.cache;
    self.caches = &caches;

    // _ = try std.Thread.spawn(.{}, healthCheck, .{self});
    try poller(socket_fd, &poll_args, &fd_conns, &self.caches, self.arena);
}

test "test memory leaks cache init" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();

    const config = Config{
        .port = 7000,
        .addr = "127.0.0.1",
        .arena = &arena,
        .replicas = 2,
        .enabled_multithread = false,
    };
    var cache: Self = undefined;
    try cache.init(config);
    defer (cache.deinit()) catch @panic("Could not deinit cache");

    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    defer posix.close(socket_fd);

    var option_value: i32 = 1; // Enable the option
    const option_value_bytes = std.mem.asBytes(&option_value);
    try posix.setsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);

    try posix.bind(socket_fd, &cache.addr.any, cache.addr.getOsSockLen());
    try posix.listen(socket_fd, 128);

    cache.health = CacheHealth.Alive;

    var poll_args = std.ArrayList(posix.pollfd).init(arena);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(arena);
    defer fd_conns.deinit();

    // var caches = cache.arena.*.alloc(*Cache, 1) catch {
    //     std.debug.print("\nFailed to alloc memory for caches", .{});
    //     return CacheError.FailedToAllocMemForSingleCache;
    // };

    // var caches: []const *Cache = undefined;
    // caches = &[_]*Cache{};
    // caches[0] = &cache.cache;
    // cache.caches = caches;

    var caches = [_]*Cache{&cache.cache};
    caches[0] = &cache.cache;
    cache.caches = &caches;

    // _ = try std.Thread.spawn(.{}, healthCheck, .{self});
    // try poller(socket_fd, &poll_args, &fd_conns, &cache.caches, cache.arena);
}
