const std = @import("std");
const print = std.debug.print;
const posix = std.posix;
const Conn = @import("cluster.zig").Conn;
const State = @import("cluster.zig").State;
const error_fns = @import("utils/error.zig");
const helpers = @import("utils/helpers.zig");
const hashKey = helpers.hashKey;
const assert = error_fns.assert_cm;
const Cache = @import("cache.zig");
const connections = @import("cache_runtime/connections.zig");
const parseCommand = connections.parseCommand;
const worker = connections.worker;

const Self = @This();

pub fn pollConnections(
    socket_fd: i32,
    poll_args_ptr: *std.ArrayList(posix.pollfd),
    fd_conns_ptr: *std.ArrayList(*Conn),
    cluster_port: u16,
    caches_ptr: *[]*Cache,
    arena: *std.mem.Allocator,
) !void {
    var poll_args = poll_args_ptr.*;
    var fd_conns = fd_conns_ptr.*;
    var caches = caches_ptr.*;
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
                    print("\nClient fd, {d} {d}", .{ std.time.nanoTimestamp(), cluster_port });
                    if (i - 1 >= fd_conns.items.len) {
                        return;
                    }
                    const conn = fd_conns.items[i - 1];
                    print("\nClient fd, {d}", .{conn.fd});
                    // std.time.sleep(1_000_000_000);

                    conn.*.start_time = std.time.nanoTimestamp();

                    assert(conn.fd > 0, "No valid connection");
                    connectionIo(conn, &caches, arena) catch |err| {
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
                try connectionIo(&conn, &caches, arena);
            }

            if (conn.state == State.RESP) {
                try connectionIo(&conn, &caches, arena);
            }

            if (conn.state == State.End) {
                assert(conn.fd > 0, "Not valid connection fd");
                // If you call conn.builder.?.len(), in a print; it frees the builder for some reason;
                if (conn.builder != null) {
                    conn.builder.?.*.deinit(arena.*);
                }
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

fn connectionIo(conn: *Conn, caches: *const []*Cache, arena: *std.mem.Allocator) !void {
    if (caches.*.len > 1) {
        try connectionIoSingleInst(conn, caches.*[0], arena);
    } else {
        try connectionIoClusterInst(conn, caches, arena);
    }
}

fn connectionIoSingleInst(conn: *Conn, cache: *Cache, arena: *std.mem.Allocator) !void {
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

fn connectionIoClusterInst(conn: *Conn, caches: *const []*Cache, arena: *std.mem.Allocator) !void {
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
                print("\nCache{d}", .{idx});
                try worker(cmd, conn, caches.*[idx], arena);
            },
            .lrange => |v| {
                const hash = hashKey(v.dll_name);
                const idx = hash % caches.len;
                print("\nCache{d}", .{idx});
                try worker(cmd, conn, caches.*[idx], arena);
            },
        }
    }

    conn.state = State.RESP;
    return;
}