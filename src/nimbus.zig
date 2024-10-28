const std = @import("std");
const posix = std.posix;
const Parser = @import("parser.zig");
const error_fns = @import("error.zig");
const Command = @import("types.zig").Command;
const print = std.debug.print;
const Cache = @import("cache.zig");
const DLL = @import("dll.zig").DLinkedList;
const Types = @import("types.zig");
const assert = error_fns.assert_cm;
const connections = @import("connections.zig");
const Conn = @import("cluster.zig").Conn;
const State = @import("cluster.zig").State;

const DEFAULT_PORT: usize = 6379;

const Self = @This();
address: []const u8,
port: u16,
allocator: *std.mem.Allocator,
addr: std.net.Address,
replicas: u16,
cache: Cache,

// For some reason we get an alias error when the buffer is set to 512
// this could be do to the internal syscall made by the kernal "posix"

pub const Config = struct {
    port: u16,
    addr: []const u8,
    allocator: *std.mem.Allocator,
    replicas: u16,
};

pub fn init(target: *Self, config: Config) !void {
    const ip_addr = try std.net.Address.parseIp4(config.addr, config.port);
    var cache: Cache = undefined;
    try cache.init(config.allocator, config.replicas);
    target.* = .{
        .address = config.addr,
        .port = config.port,
        .allocator = config.allocator,
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
    //
    try posix.bind(socket_fd, &self.addr.any, self.addr.getOsSockLen());
    try posix.listen(socket_fd, 128);

    print("Running Nimbus Cache on {s}:{d}...\n", .{ self.address, self.port });

    // const poll_args: []posix.pollfd = undefined;
    var poll_args = std.ArrayList(posix.pollfd).init(std.heap.page_allocator);
    defer poll_args.deinit();
    var fd_conns = std.ArrayList(*Conn).init(std.heap.page_allocator);
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
                    connectionIo(conn, &self.cache, self.allocator) catch |err| {
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
                try connectionIo(&conn, &self.cache, self.allocator);
            }

            if (conn.state == State.RESP) {
                try connectionIo(&conn, &self.cache, self.allocator);
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

fn worker(conn: *Conn, cache: *Cache, allocator: *std.mem.Allocator) !void {
    print("\nClient sent: \n{s}", .{conn.rbuf[0..conn.rbuf_size]});
    // var current_time = std.time.nanoTimestamp();

    var msg = try Parser.parse(&conn.rbuf, std.heap.page_allocator);
    const command = try msg.toCommand();
    if (command) |cmd| {
        switch (cmd) {
            .ping => {
                conn.buffer = "PONG";
                // try dispatch(conn);
            },
            .echo => |v| {
                const response = try std.fmt.allocPrint(std.heap.c_allocator, "${}\r\n{s}\r\n", .{ v.len, v });
                conn.buffer = response;
                // try dispatch(conn, response);
            },
            .set => |v| {
                cache.set(v.key, v.value) catch {
                    conn.buffer = "-ERROR";
                    return;
                };
                conn.buffer = "+OK";
                // try dispatch(conn, "+OK");
            },
            .get => |v| {
                const entry = cache.get(v.key);
                if (entry != null) {
                    const response = try std.fmt.allocPrint(
                        std.heap.c_allocator,
                        "${}\r\n{s}\r\n",
                        .{ entry.?.value.string.len, entry.?.value.string },
                    );
                    conn.buffer = response;
                    // try dispatch(conn, response);
                } else {
                    conn.buffer = "-ERROR";
                }
            },
            .lpush => |v| {
                const entry = cache.get(v.dll_name);
                var response: []const u8 = ":1\r\n";
                if (entry == null) {
                    const dll = try allocator.create(DLL);
                    dll.* = DLL.init(std.heap.page_allocator);
                    try dll.*.addFront(v.dll_new_value);
                    const dll_resp = Types.RESP{ .dll = dll };
                    try cache.set(v.dll_name, dll_resp);
                } else if (entry != null) {
                    const dll = entry.?.value.dll;
                    try dll.*.addFront(v.dll_new_value);

                    response = try std.fmt.allocPrint(
                        std.heap.c_allocator,
                        ":{}\r\n",
                        .{dll.size},
                    );
                }
                conn.buffer = response;
            },
            .lpushmany => |v| {
                const entry = cache.get(v.dll_name);
                var response: []const u8 = ":1\r\n";
                if (entry == null) {
                    const dll = try allocator.create(DLL);
                    dll.* = DLL.init(std.heap.page_allocator);
                    for (v.dll_values) |value| {
                        try dll.*.addBack(value);
                    }
                    const dll_resp = Types.RESP{ .dll = dll };
                    try cache.set(v.dll_name, dll_resp);
                } else if (entry != null) {
                    const dll = entry.?.value.dll;
                    for (v.dll_values) |value| {
                        try dll.*.addBack(value);
                    }

                    response = try std.fmt.allocPrint(
                        std.heap.c_allocator,
                        ":{}\r\n",
                        .{dll.size},
                    );
                }
                conn.buffer = response;
            },
            .lrange => |v| {
                const entry = cache.get(v.dll_name);
                if (entry != null) {
                    const dll = entry.?.value.dll;
                    var size: usize = dll.*.size;
                    const start: usize = @intCast(v.start_index);
                    const additional: usize = @intCast(@abs(v.end_range));

                    if (additional > size or start >= size) {
                        conn.buffer = "-ERROR INDEX RANGE";
                    } else {
                        if (v.end_range < -1) {
                            size -= additional - 1;
                        }

                        var builder: std.RingBuffer = try std.RingBuffer.init(std.heap.c_allocator, 1024);
                        const addition: []const u8 = try std.fmt.allocPrint(
                            std.heap.c_allocator,
                            "*{}\r\n",
                            .{size},
                        );
                        try builder.writeSlice(addition);
                        var node = dll.*.head.?;
                        for (0..size) |i| {
                            if (i >= start) {
                                const value: []const u8 = try std.fmt.allocPrint(
                                    std.heap.c_allocator,
                                    "${}\r\n{s}\r\n",
                                    .{ size, node.*.value },
                                );
                                try builder.writeSlice(value);
                            }
                            if (node.*.next != null) {
                                node = node.*.next.?;
                            }
                        }
                        const len = builder.len();
                        conn.buffer = builder.data[0..len];
                    }
                } else {
                    conn.buffer = "-ERROR";
                }
            },
        }
    }
    conn.state = State.RESP;
    return;
}

fn connectionIo(conn: *Conn, cache: *Cache, allocator: *std.mem.Allocator) !void {
    switch (conn.state) {
        State.REQ => {
            print("\nreq state\n", .{});
            try connections.stateReq(conn);
        },
        State.Processing => {
            worker(conn, cache, allocator) catch |err| {
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
