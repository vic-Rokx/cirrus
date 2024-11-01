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
const helpers = @import("../utils/helpers.zig");
const handles = @import("command_handles.zig");
const hashKey = helpers.hashKey;
pub fn acceptNewConnection(
    fd_conns: *std.ArrayList(*Conn),
    socket_fd: posix.socket_t,
) !void {
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
        .builder = undefined,
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

fn callCommand(
    cmd: Command,
    pos_command: *u16,
    conn: *Conn,
    caches: *const []*Cache,
    arena: *std.mem.Allocator,
) !void {
    switch (cmd) {
        .ping => {
            try worker(cmd, conn, caches.*[0], arena);
            try stateResp(conn);
            pos_command.* += 1;
        },
        .echo => |v| {
            const hash = hashKey(v);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 2;
        },
        .set => |v| {
            const hash = hashKey(v.key);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 3;
        },
        .get => |v| {
            const hash = hashKey(v.key);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 2;
        },
        .del => |v| {
            const hash = hashKey(v.key);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 2;
        },
        .lpush => |v| {
            const hash = hashKey(v.dll_name);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 3;
        },
        .lpushmany => |v| {
            const hash = hashKey(v.dll_name);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            const num_values: u16 = @intCast(v.dll_values.len);
            pos_command.* += num_values + 2;
        },
        .lrange => |v| {
            const hash = hashKey(v.dll_name);
            const idx = hash % caches.len;
            try worker(cmd, conn, caches.*[idx], arena);
            try stateResp(conn);
            pos_command.* += 4;
        },
    }
}

pub fn parseMsg(conn: *Conn, caches: *const []*Cache, arena: *std.mem.Allocator) !void {
    const msg = try Parser.parse(conn.*.rbuf[0..conn.rbuf_size], arena);

    var pos_command: u16 = 0;
    switch (msg) {
        .array => {
            var msg_copy = msg;
            const len = msg.array.values.len;
            while (pos_command < len) {
                msg_copy.array.values = msg.array.values[pos_command..];
                const cmd = try msg_copy.toCommand();
                if (cmd != null) {
                    print("Calling worker", .{});
                    try callCommand(cmd.?, &pos_command, conn, caches, arena);
                }
            }
        },
        else => {
            const cmd = try msg.toCommand();
            if (cmd != null) {
                try callCommand(cmd.?, &pos_command, conn, caches, arena);
            }
        },
    }
}

pub fn worker(cmd: Command, conn: *Conn, cache: *Cache, arena: *std.mem.Allocator) !void {
    switch (cmd) {
        .ping => {
            handles.handlePing(conn);
        },
        .echo => |v| {
            try handles.handleEcho(conn, arena, v);
        },
        .set => |v| {
            handles.handleSet(conn, cache, v.key, v.value);
        },
        .get => |v| {
            try handles.handleGet(conn, cache, arena, v.key);
        },
        .del => |v| {
            handles.handleDel(conn, cache, v.key);
        },
        .lpush => |v| {
            try handles.handleLpush(conn, cache, arena, v.dll_name, v.dll_new_value);
        },
        .lpushmany => |v| {
            try handles.handleLpushMany(conn, cache, arena, v.dll_name, v.dll_values);
        },
        .lrange => |v| {
            try handles.handleLrange(conn, cache, arena, v.dll_name, v.start_index, v.end_range);
        },
    }
}

pub fn dispatch(conn: *Conn) !void {
    print("\nDispatching request{s}", .{conn.buffer[0..]});
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

    // print("Filling the buffer", .{});
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
