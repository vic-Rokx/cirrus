const std = @import("std");
const Types = @import("types.zig");
const RESP = Types.RESP;
const Command = Types.Command;
const Self = @This();
const testing = std.testing;

input: []const u8,
position: usize = 0,
arena: *std.mem.Allocator,

pub fn parse(buffer: []const u8, allocator: *std.mem.Allocator) !RESP {
    var parser = Self{ .input = buffer, .arena = allocator };
    return (&parser).parseReq();
}

pub fn peek(self: *Self) !u8 {
    return if (self.position >= self.input.len) error.EOF else self.input[self.position];
}

test "Peek function test" {
    const req = "peek";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    var c = try parser.peek();
    try testing.expect(c == 'p');
    parser.position += 1;
    c = try parser.peek();
    try testing.expect(c == 'e');
    parser.position += 1;
    c = try parser.peek();
    try testing.expect(c == 'e');
    parser.position += 1;
    c = try parser.peek();
    try testing.expect(c == 'k');
    parser.position += 1;
    const err = parser.peek();
    try testing.expectError(error.EOF, err);
}

fn pop(self: *Self) !u8 {
    const c = try self.peek();
    self.position += 1;
    return c;
}

test "Pop function test" {
    const req = "peek";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    var c = try parser.pop();
    try testing.expect(c == 'p');
    c = try parser.pop();
    try testing.expect(c == 'e');
    c = try parser.pop();
    try testing.expect(c == 'e');
    c = try parser.pop();
    try testing.expect(c == 'k');
    const err = parser.pop();
    try testing.expectError(error.EOF, err);
}

fn parseAndConsumeLengthPrefix(self: *Self) !usize {
    var len: usize = 0;

    while (true) {
        // Here we pop the length off, position = 2
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

fn parseConsumeReturnInt(self: *Self) !i32 {
    var int: i32 = 0;

    while (true) {
        // Here we pop the length off, position = 2
        const v = try self.pop();
        // '0' in ASCII is 48, and '9' is 57, this way we can parse char to usize
        switch (v) {
            '0'...'9' => {
                int *= 10;
                // v = '1' = 49 in ASCII and '0' = 48 so 49 - 48 = 1
                int += v - '0';
            },
            '\r' => {
                // we pop the '\n' since it follows '\r'
                _ = try self.pop();
                return int;
            },
            else => return error.EOF,
        }
    }
}

test "ParseAndConsumeLengthPrefix function test" {
    const req = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    // Pop the type off the string;
    var c = try parser.pop();
    try testing.expect(c == '*');
    var len = try parser.parseAndConsumeLengthPrefix();
    try testing.expect(len == 2);
    try testing.expect(parser.position == 4);

    c = try parser.pop();
    try testing.expect(c == '$');
    len = try parser.parseAndConsumeLengthPrefix();
    try testing.expect(len == 5);
    try testing.expect(parser.position == 8);

    parser.position += 7;
    c = try parser.pop();
    try testing.expect(c == '$');
    len = try parser.parseAndConsumeLengthPrefix();
    try testing.expect(len == 5);
    try testing.expect(parser.position == 19);

    const err = parser.parseAndConsumeLengthPrefix();
    try testing.expectError(error.EOF, err);
}

pub fn parseReq(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    const resp_type = try self.peek();

    return switch (resp_type) {
        ':' => self.parseNumber(),
        '+' => self.parseInt(),
        '$' => self.parseBulkString(),
        '*' => self.parseArray(),
        else => error.unexpected,
    };
}

test "ParseReq function test" {
    var req: []const u8 = ":-232\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    var resp = try parser.parseReq();
    try testing.expectEqualDeep(RESP{ .int = -232 }, resp);

    req = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    parser = Self{ .input = req, .arena = &arena };
    resp = try parser.parseReq();
    var resp_arr = [_]RESP{ RESP{ .string = "hello" }, RESP{ .string = "world" } };
    try testing.expectEqualDeep(RESP{ .array = .{
        .values = &resp_arr,
        .arena = &arena,
    } }, resp);
}

fn parseIntSign(self: *Self) !i32 {
    // Get the sign character ('+' or '-')
    _ = try self.pop();
    const sign_char = try self.pop();
    return switch (sign_char) {
        '+' => 1,
        '-' => -1,
        else => return error.unexpected,
    };
}

fn parseNumber(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    const sign = try self.parseIntSign();
    var number = try self.parseConsumeReturnInt();
    number = number * sign;
    return RESP{ .int = number };
}

fn parseInt(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    _ = try self.pop();
    const len = try self.parseAndConsumeLengthPrefix();
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
    const len = try self.parseAndConsumeLengthPrefix();
    const arr = RESP{
        .array = .{
            .arena = self.arena,
            .values = try self.arena.*.alloc(RESP, len),
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
    const len = try self.parseAndConsumeLengthPrefix();
    const s = try self.arena.*.alloc(u8, len);
    std.mem.copyForwards(u8, s, self.input[self.position..(self.position + len)]);
    self.position += len;
    try self.popTerminator();
    return RESP{ .string = s };
}

fn popTerminator(self: *Self) !void {
    _ = try self.pop();
    _ = try self.pop();
}

test "parse array bulk string" {
    const req = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp.toCommand(), Command{ .echo = "hey" });
}

test "parse bulk string array" {
    const req = "$4\r\necho\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp, RESP{ .string = "echo" });
}

test "parse and call command" {
    const req = "*2\r\n$4\r\nECHO\r\n$3\r\nVic\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp.toCommand(), Command{ .echo = "Vic" });
}

// test "LPUSH Command" {
//     const req = "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n";
//     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
//     defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
//     var arena = gpa.allocator();
//     const resp = try Self.parse(req, &arena);
//     try testing.expectEqualDeep(
//         resp.toCommand(),
//         Command{ .lpush = .{ .dll_name = "mylist", .dll_new_value = .  },
//     );
// }

// test "LRANGE Command" {}
