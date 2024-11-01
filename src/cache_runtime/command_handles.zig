const std = @import("std");
const Conn = @import("../cluster.zig").Conn;
const Command = @import("../types.zig").Command;
const Cache = @import("../cache.zig");
const Types = @import("../types.zig");
const DLL = @import("../storage/dll.zig").DLinkedList;

pub fn handlePing(conn: *Conn) void {
    conn.buffer = "PONG";
}

pub fn handleEcho(conn: *Conn, arena: *std.mem.Allocator, value: []const u8) !void {
    const response = try std.fmt.allocPrint(arena.*, "${}\r\n{s}\r\n", .{ value.len, value });
    conn.buffer = response;
}

pub fn handleSet(conn: *Conn, cache: *Cache, key: []const u8, value: Types.RESP) void {
    cache.*.set(key, value) catch {
        conn.buffer = "-ERROR";
        return;
    };
    conn.buffer = "+OK";
}

pub fn handleGet(conn: *Conn, cache: *Cache, arena: *std.mem.Allocator, key: []const u8) !void {
    const entry = cache.*.get(key);
    if (entry != null) {
        const response = try std.fmt.allocPrint(
            arena.*,
            "${}\r\n{s}\r\n",
            .{ entry.?.value.string.len, entry.?.value.string },
        );
        conn.buffer = response;
    } else {
        conn.buffer = "-ERROR";
    }
}

pub fn handleDel(conn: *Conn, cache: *Cache, key: []const u8) void {
    const entry_exists = cache.*.del(key);
    conn.buffer = if (entry_exists) "+OK" else "-NULL";
}

pub fn handleLpush(conn: *Conn, cache: *Cache, arena: *std.mem.Allocator, dll_name: []const u8, dll_value: []const u8) !void {
    const entry = cache.*.get(dll_name);
    var response: []const u8 = ":1\r\n";
    if (entry == null) {
        const dll = try arena.*.create(DLL);
        dll.* = DLL.init(arena.*);
        try dll.*.addBack(dll_value);
        const dll_resp = Types.RESP{ .dll = dll };
        try cache.*.set(dll_name, dll_resp);
    } else if (entry != null) {
        const dll = entry.?.value.dll;
        try dll.*.addBack(dll_value);

        response = try std.fmt.allocPrint(
            arena.*,
            ":{}\r\n",
            .{dll.size},
        );
    }
    conn.buffer = response;
}

pub fn handleLpushMany(conn: *Conn, cache: *Cache, arena: *std.mem.Allocator, dll_name: []const u8, dll_values: [][]const u8) !void {
    const entry = cache.*.get(dll_name);
    var response: []const u8 = ":1\r\n";
    if (entry == null) {
        const dll = try arena.*.create(DLL);
        dll.* = DLL.init(arena.*);
        for (dll_values) |value| {
            try dll.*.addBack(value);
        }
        const dll_resp = Types.RESP{ .dll = dll };
        response = try std.fmt.allocPrint(
            arena.*,
            ":{}\r\n",
            .{dll.size},
        );

        try cache.*.set(dll_name, dll_resp);
    } else if (entry != null) {
        const dll = entry.?.value.dll;
        for (dll_values) |value| {
            try dll.*.addBack(value);
        }

        response = try std.fmt.allocPrint(
            arena.*,
            ":{}\r\n",
            .{dll.size},
        );

        const dll_resp = Types.RESP{ .dll = dll };
        try cache.*.set(dll_name, dll_resp);
    }
    conn.buffer = response;
}

pub fn handleLrange(
    conn: *Conn,
    cache: *Cache,
    arena: *std.mem.Allocator,
    dll_name: []const u8,
    start_index: i32,
    end_range: i32,
) !void {
    const entry = cache.*.get(dll_name);
    if (entry != null) {
        const dll = entry.?.value.dll;
        var size: usize = dll.*.size;
        const start: usize = @intCast(start_index);
        const additional: usize = @intCast(@abs(end_range));
        if (additional > size or start >= size) {
            conn.buffer = "-ERROR INDEX RANGE";
        } else {
            if (end_range < -1) {
                size -= additional - 1;
            }
            var builder = try std.RingBuffer.init(arena.*, 1024);
            const addition: []const u8 = try std.fmt.allocPrint(
                arena.*,
                "*{}\r\n",
                .{size},
            );
            try builder.writeSlice(addition);
            var node = dll.*.head.?;
            for (0..size) |i| {
                if (i >= start) {
                    const value: []const u8 = try std.fmt.allocPrint(
                        arena.*,
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
            conn.builder = &builder;
            conn.buffer = builder.data[0..len];
        }
    } else {
        conn.buffer = "-ERROR";
    }
}
