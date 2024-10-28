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

        // std.time.sleep(1_000_000_000);
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
                    print("\nClient fd, {d} {d}", .{ std.time.nanoTimestamp(), self.port });
                    if (i - 1 >= fd_conns.items.len) {
                        return;
                    }
                    const conn = fd_conns.items[i - 1];
                    print("\nClient fd, {d}", .{conn.fd});
                    // std.time.sleep(1_000_000_000);

                    conn.*.start_time = std.time.nanoTimestamp();

                    assert(conn.fd > 0, "No valid connection");
                    connectionIo(conn, &self.cache, self.arena) catch |err| {
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
                try connectionIo(&conn, &self.cache, self.arena);
            }

            if (conn.state == State.RESP) {
                try connectionIo(&conn, &self.cache, self.arena);
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

fn connectionIo(conn: *Conn, cache: *Cache, arena: *std.mem.Allocator) !void {
    switch (conn.state) {
        State.REQ => {
            print("\nreq state\n", .{});
            try connections.stateReq(conn);
        },
        State.Processing => {
            const command = parseCommand(conn, arena) catch {
                return error.ReadFailed;
            };
            if (command) |cmd| {
                worker(cmd, conn, cache, arena) catch |err| {
                    print("\nWorker event error: {any}", .{err});
                    return error.ReadFailed;
                };
            }
            conn.state = State.RESP;
            return;
        },
        State.RESP => {
            try connections.stateResp(conn);
        },
        State.End => {},
    }
}
