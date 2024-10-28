const std = @import("std");
const Types = @import("types.zig");
const RESP = Types.RESP;
const Command = Types.Command;
const Self = @This();
const testing = std.testing;

input: []const u8,
position: usize = 0,
allocator: std.mem.Allocator,

pub fn parse(buffer: []const u8, allocator: std.mem.Allocator) !RESP {
    var parser = Self{ .input = buffer, .allocator = allocator };
    // return try parser.parseReq();
    return (&parser).parseReq();
}

pub fn peek(self: *Self) !u8 {
    return if (self.position >= self.input.len) error.EOF else self.input[self.position];
}

fn pop(self: *Self) !u8 {
    const c = try self.peek();
    self.position += 1;
    return c;
}

fn parseLengthAndRemove(self: *Self) !usize {
    var len: usize = 0;

    while (true) {
        // Here we pop the length off position = 2
        const v = try self.pop();
        // '0' in ASCII is 48, and '9' is 57, this way we can parse char to usize
        switch (v) {
            '0'...'9' => {
                len *= 10;
                // v = '1' = 49 in ASCII and '0' = 48 so 49 - 48 = 1
                len += v - '0';
            },
            '\r' => {
                // we pop the '\n' since it follows '\r'
                _ = try self.pop();
                return len;
            },
            else => return error.EOF,
        }
    }
}

pub fn parseReq(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    const resp_type = try self.peek();

    return switch (resp_type) {
        '+' => self.parseInt(),
        '$' => self.parseBulkString(),
        '*' => self.parseArray(),
        else => error.unexpected,
    };
}

fn parseInt(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    _ = try self.pop();
    const len = try self.parseLengthAndRemove();
    const value = self.input[self.position..(self.position + len)];
    const integer = std.fmt.parseInt(i32, value, 10) catch {
        return error.unexpected;
    };
    self.position += len;
    try self.popTerminator();
    return RESP{ .int = integer };
}

// *<number-of-elements>\r\n<element-1>...<element-n>
// *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
fn parseArray(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Remove the resp_type
    _ = try self.pop();
    const len = try self.parseLengthAndRemove();
    const arr = RESP{
        .array = .{
            .allocator = self.allocator,
            .values = try self.allocator.alloc(RESP, len),
        },
    };

    for (0..len) |i| {
        arr.array.values[i] = try self.parseReq();
    }

    return arr;
}

fn parseBulkString(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Here we pop '$' => position + 1 = 1;
    _ = try self.pop();
    const len = try self.parseLengthAndRemove();
    const s = try self.allocator.alloc(u8, len);
    std.mem.copyForwards(u8, s, self.input[self.position..(self.position + len)]);
    self.position += len;
    try self.popTerminator();
    return RESP{ .string = s };
}

fn popTerminator(self: *Self) !void {
    _ = try self.pop();
    _ = try self.pop();
}

// test "parse array bulk string" {
//     const req = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
//     const resp = try Self.parse(req, std.heap.page_allocator);
//     try testing.expectEqualDeep(resp.toCommand().?, Command{ .echo = "hey" });
//     std.debug.print("\n", .{});
//     for (resp.array.values) |item| {
//         std.debug.print("{s}\n", .{item.string});
//     }
// }
//
// test "parse bulk string array" {
//     const req = "$4\r\necho\r\n";
//     const resp = try Self.parse(req, std.heap.page_allocator);
//     try testing.expectEqualDeep(resp, RESP{ .string = "echo" });
//     std.debug.print("\n{s}\n", .{resp.string});
// }
//
// test "parse and call command" {
//     const req = "*2\r\n$4\r\nECHO\r\n$3\r\nVic\r\n";
//     const resp = try Self.parse(req, std.heap.page_allocator);
//     try testing.expectEqualDeep(resp.toCommand().?, Command{ .echo = "Vic" });
// }
//
// test "LPUSH Command" {
//     const req = "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n";
//     const resp = try Self.parse(req, std.heap.page_allocator);
//     try testing.expectEqualDeep(
//         resp.toCommand().?,
//         Command{ .lpush = .{ .len = 1 } },
//     );
// }
//
// test "LRANGE Command" {}
