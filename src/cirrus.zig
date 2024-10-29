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
const parseCommand = connections.parseCommand;
const poller = @import("poller.zig").pollConnections;

const DEFAULT_PORT: usize = 6379;

const Self = @This();
address: []const u8,
port: u16,
arena: *std.mem.Allocator,
addr: std.net.Address,
replicas: u16,
cache: Cache,

// For some reason we get an alias error when the buffer is set to 512
// this could be do to the internal syscall made by the kernal "posix"

pub const Config = struct {
    port: u16,
    addr: []const u8,
    arena: *std.mem.Allocator,
    replicas: u16,
};

const CacheError = error{
    FailedToAllocMemForSingleCache,
};

pub fn init(target: *Self, config: Config) !void {
    const ip_addr = try std.net.Address.parseIp4(config.addr, config.port);
    var cache: Cache = undefined;
    try cache.init(config.arena, config.replicas);
    target.* = .{
        .address = config.addr,
        .port = config.port,
        .arena = config.arena,
        .addr = ip_addr,
        .replicas = config.replicas,
        .cache = cache,
    };
}

pub fn deinit(self: *Self) !void {
    self.cache.deinit();
}

pub fn run(self: *Self) !void {
    const socket_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, posix.IPPROTO.TCP);
    // print("Socket fd: {d}\n", .{socket_fd});
    defer posix.close(socket_fd);

    defer self.cache.deinit();

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

    var poll_args = std.ArrayList(posix.pollfd).init(self.arena.*);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(self.arena.*);
    defer fd_conns.deinit();
    var caches = self.arena.*.alloc(*Cache, 1) catch {
        std.debug.print("\nFailed to alloc memory for caches", .{});
        return CacheError.FailedToAllocMemForSingleCache;
    };
    caches[0] = &self.cache;

    try poller(socket_fd, &poll_args, &fd_conns, self.port, &caches, self.arena);
}
