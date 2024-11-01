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

pub fn parseReq(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    const resp_type = try self.peek();

    return switch (resp_type) {
        ':' => self.parseInt(),
        '+' => self.parseSimpleString(),
        '$' => self.parseBulkString(),
        '*' => self.parseArray(),
        ',' => self.parseFloat(),
        '#' => self.parseBool(),
        '%' => self.parseMap(),
        // '~' => self.parseSet(),
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

fn parseInt(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    const sign = try self.parseIntSign();
    var integer = try self.parseConsumeReturnInt();
    integer = integer * sign;
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

fn parseSimpleString(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Here we pop '+' => position + 1 = 1;
    _ = try self.pop();

    // Slice to the end crlf
    const simple_str: []const u8 = std.mem.sliceTo(self.input[self.position..], '\r');
    // increment the position by the len of the string;
    self.position += simple_str.len;
    // pop off the \r\n;
    _ = try self.pop();
    _ = try self.pop();

    return RESP{ .string = simple_str };
}

test "ParseReq simple string test" {
    const req: []const u8 = "+NIMBUS\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    const resp = try parser.parseReq();
    try testing.expectEqualDeep(RESP{ .string = "NIMBUS" }, resp);
}

fn parseBulkString(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Here we pop '$' => position + 1 = 1;
    _ = try self.pop();
    const len = try self.parseAndConsumeLengthPrefix();
    const value = self.input[self.position..(self.position + len)];

    const integer = std.fmt.parseInt(i32, value, 10) catch {
        const s = self.arena.*.alloc(u8, len) catch {
            return error.OutOfMemory;
        };
        std.mem.copyForwards(u8, s, value);
        self.position += len;
        try self.popTerminator();
        return RESP{ .string = s };
    };
    self.position += len;
    try self.popTerminator();
    return RESP{ .int = integer };
}

test "parse array bulk string" {
    const req = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp.toCommand(), Command{ .echo = "hey" });
}

fn parseFloat(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // pop ','
    _ = try self.pop();
    // iterate till '\r'
    var len: u16 = 0;
    while (true) {
        const v = try self.pop();
        switch (v) {
            '0'...'9' => {
                len += 1;
            },
            '.' => {
                len += 1;
            },
            '\r' => {
                // we pop the '\n' since it follows '\r'
                _ = try self.pop();
                break;
            },
            else => return error.EOF,
        }
    }

    const float = std.fmt.parseFloat(f32, self.input[1 .. len + 1]) catch {
        return error.unexpected;
    };

    return RESP{ .float = float };
}

test "ParseReq float test" {
    const req: []const u8 = ",2.32\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    const resp = try parser.parseReq();
    try testing.expectEqualDeep(RESP{ .float = 2.32 }, resp);
}

fn parseBool(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Here we pop '#' => position + 1 = 1;
    _ = try self.pop();
    const c = try self.pop();
    const boolean = switch (c) {
        't' => true,
        'f' => false,
        else => return error.EOF,
    };
    return RESP{ .boolean = boolean };
}

test "ParseReq boolean test" {
    const req: []const u8 = "#t\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    const resp = try parser.parseReq();
    try testing.expectEqualDeep(RESP{ .boolean = true }, resp);
}

// %2\r\n+first\r\n:+1\r\n+second\r\n:+2\r\n
fn parseMap(self: *Self) error{ EOF, unexpected, OutOfMemory }!RESP {
    // Remove the resp_type %
    _ = try self.pop();
    const len = try self.parseAndConsumeLengthPrefix();
    const key_value_arr = try self.arena.*.alloc(RESP, len * 2);
    for (0..len * 2) |i| {
        key_value_arr[i] = try self.parseReq();
    }
    const map_ptr = try self.arena.*.create(std.StringHashMap(RESP));
    map_ptr.* = std.StringHashMap(RESP).init(self.arena.*);

    var idx: u16 = 0;
    for (key_value_arr) |_| {
        if (idx >= len * 2) break;
        try map_ptr.*.put(key_value_arr[idx].string, key_value_arr[idx + 1]);
        idx += 2;
    }

    return RESP{ .map = map_ptr };
}

fn compareHashMapContent(
    map1: *std.StringHashMap(RESP),
    map2: *std.StringHashMap(RESP),
) !bool {
    if (map1.count() != map2.count()) return false;

    var iter = map1.iterator();
    var iter2 = map2.iterator();
    while (iter.next()) |entry| {
        const entry2 = iter2.next().?;
        const value1 = entry.value_ptr.*;
        const value2 = entry2.value_ptr.*;
        try std.testing.expectEqualDeep(value1, value2);
    }

    return true;
}

test "ParseReq map test" {
    const req: []const u8 = "%2\r\n+first\r\n:+1\r\n+second\r\n:+2\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = gpa.allocator();
    var parser = Self{ .input = req, .arena = &arena };
    const resp = try parser.parseReq();
    var map = std.StringHashMap(RESP).init(arena);
    try map.put("first", RESP{ .int = 1 });
    try map.put("second", RESP{ .int = 2 });
    try std.testing.expect(try compareHashMapContent(&map, resp.map));
}

fn popTerminator(self: *Self) !void {
    _ = try self.pop();
    _ = try self.pop();
}

test "parse bulk string array" {
    const req = "$4\r\necho\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp, RESP{ .string = "echo" });
}

test "Parse bulk string array" {
    const req = "*6\r\n$3\r\nSET\r\n$3\r\nage\r\n:+12\r\n$3\r\nSET\r\n$4\r\nname\r\n+Vic\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    var arr_set = [_]RESP{
        RESP{ .string = "SET" },
        RESP{ .string = "age" },
        RESP{ .int = 12 },
        RESP{ .string = "SET" },
        RESP{ .string = "name" },
        RESP{ .string = "Vic" },
    };
    const expected_resp = RESP{ .array = .{ .values = &arr_set, .arena = &arena } };
    try testing.expectEqualDeep(resp, expected_resp);
    std.debug.print("\n{any}", .{resp.toCommand()});
    try testing.expectEqualDeep(resp.toCommand(), expected_resp.toCommand());
}

test "parse and call command" {
    const req = "*2\r\n$4\r\nECHO\r\n$3\r\nVic\r\n";
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var arena = gpa.allocator();
    const resp = try Self.parse(req, &arena);
    try testing.expectEqualDeep(resp.toCommand(), Command{ .echo = "Vic" });
}
