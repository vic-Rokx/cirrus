const std = @import("std");
const DLinkedList = @import("storage/dll.zig").DLinkedList;

pub const RESP = union(enum) {
    const Self = @This();
    array: struct { values: []RESP, arena: *std.mem.Allocator },
    string: []const u8,
    int: i32,
    dll: *DLinkedList,
    float: f32,
    boolean: bool,
    map: *std.StringHashMap(RESP),

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
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "DEL")) {
                    return Command{ .del = .{
                        .key = v.values[1].string,
                    } };
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "LPUSH")) {
                    return Command{ .lpush = .{
                        .dll_name = v.values[1].string,
                        .dll_new_value = v.values[2].string,
                    } };
                }
                if (std.ascii.eqlIgnoreCase(v.values[0].string, "LPUSHMANY")) {
                    const len = v.values.len;
                    const adj_len = v.values.len - 2;
                    const arr_str = try self.array.arena.*.alloc([]const u8, adj_len);

                    for (2..len) |i| {
                        arr_str[i - 2] = v.values[i].string;
                    }

                    return Command{ .lpushmany = .{
                        .dll_name = v.values[1].string,
                        .dll_values = arr_str,
                    } };
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
            .float => |_| {
                return null;
            },
            .boolean => |_| {
                return null;
            },
            .map => |_| {
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
    del: struct { key: []const u8 },
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

test "multi command" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var arr_set = [_]RESP{
        RESP{ .string = "SET" },
        RESP{ .string = "age" },
        RESP{ .int = 12 },
        RESP{ .string = "SET" },
        RESP{ .string = "name" },
        RESP{ .string = "Vic" },
        RESP{ .string = "SET" },
        RESP{ .string = "height" },
        RESP{ .int = 175 },
        RESP{ .string = "LPUSH" },
        RESP{ .string = "DLLNAME" },
        RESP{ .string = "DLLVALUE" },
        RESP{ .string = "GET" },
        RESP{ .string = "name" },
        RESP{ .string = "LRANGE" },
        RESP{ .string = "DLLNAME" },
        RESP{ .int = 0 },
        RESP{ .int = 1 },
        RESP{ .string = "LPUSHMANY" },
        RESP{ .string = "DLLNAME" },
        RESP{ .string = "one" },
        RESP{ .string = "two" },
        RESP{ .string = "three" },
        RESP{ .string = "four" },
    };
    const resp = RESP{ .array = .{ .values = &arr_set, .arena = &arena } };
    var cmd_values = resp;
    const len = resp.array.values.len;
    var pos_command: u16 = 0;
    switch (resp) {
        .array => {
            while (pos_command < len) {
                cmd_values.array.values = resp.array.values[pos_command..];
                const cmd = try cmd_values.toCommand();
                // std.debug.print("\narray: {any}\n", .{cmd_values});
                std.debug.print("\nCommand: {any}\n", .{cmd.?});
                std.debug.print("\npos: {d}\n", .{pos_command});
                switch (cmd.?) {
                    .ping => {
                        pos_command += 1;
                    },
                    .echo => {
                        pos_command += 2;
                    },
                    .set => {
                        pos_command += 3;
                    },
                    .get => {
                        pos_command += 2;
                    },
                    .del => {
                        pos_command += 2;
                    },
                    .lpush => {
                        pos_command += 3;
                    },
                    .lpushmany => |v| {
                        pos_command += 2;
                        const len_v: u16 = @intCast(v.dll_values.len);
                        pos_command += len_v;
                    },
                    .lrange => {
                        pos_command += 4;
                    },
                }
            }
        },
        else => {},
    }
}

test "test to Command" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();

    var arr_set = [_]RESP{
        RESP{ .string = "SET" },
        RESP{ .string = "name" },
        RESP{ .string = "Vic" },
    };
    var resp = RESP{ .array = .{ .values = &arr_set, .arena = &arena } };

    var cmd = resp.toCommand();
    var command = Command{ .set = .{
        .key = "name",
        .value = RESP{ .string = "Vic" },
    } };
    try std.testing.expectEqualDeep(command, cmd);

    var arr_get = [_]RESP{
        RESP{ .string = "GET" },
        RESP{ .string = "name" },
    };

    resp = RESP{ .array = .{ .values = &arr_get, .arena = &arena } };

    cmd = resp.toCommand();
    command = Command{ .get = .{
        .key = "name",
    } };
    try std.testing.expectEqualDeep(command, cmd);
}
