const std = @import("std");
const error_fns = @import("../utils/error.zig");
const assert = error_fns.assert_cm;
const print = std.debug.print;
const posix = std.posix;
const Conn = @import("../cluster.zig").Conn;
const Command = @import("../types.zig").Command;
const Cache = @import("../cache.zig");
const State = @import("../cluster.zig").State;
const DLL = @import("../storage/dll.zig").DLinkedList;
const Types = @import("../types.zig");
const Parser = @import("../parser.zig");
pub fn acceptNewConnection(fd_conns: *std.ArrayList(*Conn), socket_fd: posix.socket_t) !void {
    var client_addr: std.net.Address = undefined;
    var client_addr_len: u32 = @intCast(@sizeOf(@TypeOf(client_addr.any)));
    const conn_fd = posix.accept(socket_fd, &client_addr.any, &client_addr_len, 0) catch |err| {
        if (err == error.WouldBlock) {
            std.time.sleep(1_000_000);
            return;
        } else {
            return error.AcceptedFailed;
        }
    };

    if (conn_fd < 0) {
        return;
    }

    print("\nNew connection id: {d}", .{conn_fd});

    var new_conn = Conn{
        .fd = conn_fd,
        .state = State.REQ,
        .rbuf_size = 0,
        .rbuf = undefined,
        .wbuf_size = 0,
        .wbuf_sent = 0,
        .wbuf = undefined,
        .buffer = undefined,
        .start_time = 0,
        .last_check = 0,
    };
    fd_conns.*.append(&new_conn) catch |err| {
        return err;
    };
    assert(fd_conns.items.len > 0, "No valid connections");
}
pub fn processConnection(conn: *Conn) void {
    print("Processing connection fd: {}\n", .{conn.fd});

    _ = try posix.read(conn.timer_fd, &conn.rbuf) catch |err| {
        print("error...{any}", .{err});
        return;
    };
    // Simulate processing delay using a non-blocking mechanism if needed
    conn.state = State.End;

    const response = std.fmt.allocPrint(std.heap.c_allocator, "+OK:{d}\r\n", .{conn.fd}) catch {
        print("Error allocating response\n", .{});
        return;
    };

    _ = posix.write(conn.fd, response) catch {
        print("Error writing response\n", .{});
        return;
    };

    if (conn.state == State.End) {
        print("Closing connection: {d}\n", .{conn.fd});
        posix.close(conn.fd);
    }
}

pub fn parseCommand(conn: *Conn, arena: *std.mem.Allocator) !?Command {
    var msg = try Parser.parse(&conn.rbuf, arena);
    const command = try msg.toCommand();
    return command;
}

pub fn worker(cmd: Command, conn: *Conn, cache: *Cache, arena: *std.mem.Allocator) !void {
    print("\nWorker parsing message: \n{s}", .{conn.rbuf[0..conn.rbuf_size]});
    // var current_time = std.time.nanoTimestamp();

    switch (cmd) {
        .ping => {
            conn.buffer = "PONG";
        },
        .echo => |v| {
            const response = try std.fmt.allocPrint(std.heap.c_allocator, "${}\r\n{s}\r\n", .{ v.len, v });
            conn.buffer = response;
        },
        .set => |v| {
            cache.*.set(v.key, v.value) catch {
                conn.buffer = "-ERROR";
                return;
            };
            conn.buffer = "+OK";
        },
        .get => |v| {
            const entry = cache.*.get(v.key);
            if (entry != null) {
                const response = try std.fmt.allocPrint(
                    std.heap.c_allocator,
                    "${}\r\n{s}\r\n",
                    .{ entry.?.value.string.len, entry.?.value.string },
                );
                conn.buffer = response;
            } else {
                conn.buffer = "-ERROR";
            }
        },
        .lpush => |v| {
            const entry = cache.*.get(v.dll_name);
            var response: []const u8 = ":1\r\n";
            if (entry == null) {
                const dll = try arena.create(DLL);
                dll.* = DLL.init(std.heap.page_allocator);
                try dll.*.addFront(v.dll_new_value);
                const dll_resp = Types.RESP{ .dll = dll };
                try cache.*.set(v.dll_name, dll_resp);
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
            const entry = cache.*.get(v.dll_name);
            var response: []const u8 = ":1\r\n";
            if (entry == null) {
                const dll = try arena.create(DLL);
                dll.* = DLL.init(std.heap.page_allocator);
                for (v.dll_values) |value| {
                    try dll.*.addBack(value);
                }
                const dll_resp = Types.RESP{ .dll = dll };
                try cache.*.set(v.dll_name, dll_resp);
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
            const entry = cache.*.get(v.dll_name);
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

pub fn dispatch(conn: *Conn) !void {
    const wv = posix.write(conn.fd, conn.buffer) catch |err| {
        print("\n Write Error: {any}", .{err});
        if (error.EINTR == err) {}
        return;
    };
    if (wv == 0) {
        conn.state = State.End;
        return;
    }

    if (wv < 0) {
        print("State error wv < 0", .{});
        return;
    }
    conn.wbuf_sent += wv;

    if (conn.wbuf_size <= conn.wbuf_sent) {
        conn.wbuf_size = 0;
        conn.wbuf_sent = 0;
        conn.state = State.End;
    }
    return;
}

pub fn fillRBuffer(conn: *Conn) !void {
    var rv: usize = 0;
    rv = posix.read(conn.fd, &conn.rbuf) catch |err| {
        if (error.EINTR == err) {
            return err;
        }
        return error.POSIXReading;
    };

    if (rv == 0) {
        conn.state = State.End;
        return error.ReadLengthZero;
    }

    if (rv > 0) {
        conn.rbuf_size += rv;
    }

    print("Filling the buffer", .{});
    conn.state = State.Processing;
    return;
    // while (tryOneReq(conn)) {}
}

pub fn tryFillBuffer(conn: *Conn) bool {
    var rv: usize = 0;
    // while (true) {
    // const cap = conn.rbuf.len - conn.rbuf_size;
    rv = posix.read(conn.fd, &conn.rbuf) catch |err| {
        if (error.EINTR == err) {
            return false;
        }
        return false;
    };

    if (rv == 0) {
        conn.state = State.End;
        return false;
    }

    if (rv > 0) {
        conn.rbuf_size += rv;
    }

    // while (tryOneReq(conn)) {}
    return (false);
    // }

    // return true;
}

pub fn tryFlushBuffer(conn: *Conn) bool {
    var rv: usize = 0;
    while (true) {
        rv = posix.write(conn.fd, "Hello") catch |err| {
            if (error.EINTR == err) {
                return false;
            }
            return false;
        };

        if (rv == 0) {
            conn.state = State.End;
            return false;
        }

        if (rv < 0) {
            print("State error rv < 0", .{});
            return false;
        }
        conn.wbuf_sent += rv;

        if (conn.wbuf_size <= conn.wbuf_sent) {
            conn.wbuf_size = 0;
            conn.wbuf_sent = 0;
            conn.state = State.REQ;
        }
    }

    return true;
}

pub fn stateReq(conn: *Conn) !void {
    try fillRBuffer(conn);
}

pub fn stateResp(conn: *Conn) !void {
    try dispatch(conn);
}
