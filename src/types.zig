const std = @import("std");
const DLinkedList = @import("dll.zig").DLinkedList;

pub const RESP = union(enum) {
    const Self = @This();
    // arrayv2: std.ArrayList(RESP),
    array: struct { values: []RESP, allocator: std.mem.Allocator },
    string: []const u8,
    int: i32,
    dll: *DLinkedList,

    pub fn toCommand(self: Self) !?Command {
        return switch (self) {
            .array => |v| {
                if (v.values.len == 1 and std.ascii.eqlIgnoreCase(v.values[0].string, "PING")) {
                    return Command{ .ping = {} };
                }
                if (v.values.len == 2 and std.ascii.eqlIgnoreCase(v.values[0].string, "ECHO")) {
                    return Command{ .echo = v.values[1].string };
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "SET")) {
                    return Command{ .set = .{
                        .key = v.values[1].string,
                        .value = v.values[2],
                    } };
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "GET")) {
                    return Command{ .get = .{
                        .key = v.values[1].string,
                    } };
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "LPUSH")) {
                    if (v.values.len > 3) {
                        const len = v.values.len;
                        const adj_len = v.values.len - 2;
                        const arr_str = try self.array.allocator.alloc([]const u8, adj_len);

                        for (2..len) |i| {
                            arr_str[i - 2] = v.values[i].string;
                        }

                        return Command{ .lpushmany = .{
                            .dll_name = v.values[1].string,
                            .dll_values = arr_str,
                        } };
                    } else {
                        return Command{ .lpush = .{
                            .dll_name = v.values[1].string,
                            .dll_new_value = v.values[2].string,
                        } };
                    }
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "LRANGE")) {
                    return Command{ .lrange = .{
                        .dll_name = v.values[1].string,
                        .start_index = v.values[2].int,
                        .end_range = v.values[3].int,
                    } };
                }
                return null;
            },
            .string => |v| {
                if (std.ascii.eqlIgnoreCase(v, "PING")) {
                    return Command{ .ping = {} };
                }
                return null;
            },
            .dll => {
                return null;
            },
            .int => |_| {
                return null;
            },
        };
    }
};

pub const Command = union(enum) {
    echo: []const u8,
    ping: void,
    get: struct { key: []const u8 },
    set: struct { key: []const u8, value: RESP },
    lpush: struct {
        dll_name: []const u8,
        dll_new_value: []const u8,
    },
    lpushmany: struct {
        dll_name: []const u8,
        dll_values: [][]const u8,
    },
    lrange: struct {
        dll_name: []const u8,
        start_index: i32,
        end_range: i32,
    },
};
