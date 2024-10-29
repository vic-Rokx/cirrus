const std = @import("std");
const crypto = std.crypto;
const fmt = std.fmt;

pub fn stringPrint(preq: []const u8, value: []const u8) void {
    std.debug.print("\n{s}: {s}\n", .{ preq, value });
}

pub fn print_pretty_address(precursor: []const u8, color: []const u8, address: std.net.Address) void {
    const reset = "\x1b[0m"; // ANSI escape code to reset color
    const bold = "\x1b[1m"; // ANSI escape code to reset color
    std.debug.print(
        "{s}{s}{s}{s} {}\n",
        .{ color, bold, precursor, reset, address },
    );
}

pub fn error_print_str(error_msg: []const u8) void {
    const boldRed = "\x1b[1;31m"; // ANSI escape code for bold + red
    const reset = "\x1b[0m"; // Reset ANSI code to clear formatting
    std.debug.print("\n{s}Error{s}: ", .{ boldRed, reset });
    std.debug.print("{s}\n", .{error_msg});
}

pub fn assert_cm(ok: bool, error_msg: []const u8) void {
    if (!ok) {
        const boldRed = "\x1b[1;31m"; // ANSI escape code for bold + red
        const reset = "\x1b[0m"; // Reset ANSI code to clear formatting
        std.debug.print("\n{s}Error{s}: ", .{ boldRed, reset });
        std.debug.print("{s}\n", .{error_msg});
        unreachable; // assertion failure
    }
}

pub fn djbHash(key: []const u8) u32 {
    // var hash: u32 = 5381;
    // for (key) |byte| {
    //     hash = ((hash << 5) +% hash) +% byte;
    // }
    // return hash;

    var h: u32 = 5381;
    for (key) |char| {
        h = ((h << 5) +% h) +% char;
    }
    h = h *% 2654435761;
    return h;
}

pub fn hashKey(key: []const u8) u32 {
    var h: u32 = 5381;
    for (key) |char| {
        h = ((h << 5) +% h) +% char;
    }
    h = h *% 2654435761;

    return h;
}

pub fn generateSessionId() [32]u8 {
    var rng = std.crypto.random;
    var session_id: [32]u8 = undefined;
    rng.bytes(&session_id);
    return session_id;
}

pub const Error = error{InvalidUUID};

pub const UUID = struct {
    bytes: [16]u8,

    pub fn init() UUID {
        var uuid = UUID{ .bytes = undefined };

        crypto.random.bytes(&uuid.bytes);
        // Version 4
        uuid.bytes[6] = (uuid.bytes[6] & 0x0f) | 0x40;
        // Variant 1
        uuid.bytes[8] = (uuid.bytes[8] & 0x3f) | 0x80;
        return uuid;
    }

    pub fn to_string(self: UUID, slice: []u8) void {
        var string: [36]u8 = format_uuid(self);
        std.mem.copyForwards(u8, slice, &string);
    }

    fn format_uuid(self: UUID) [36]u8 {
        var buf: [36]u8 = undefined;
        buf[8] = '-';
        buf[13] = '-';
        buf[18] = '-';
        buf[23] = '-';
        inline for (encoded_pos, 0..) |i, j| {
            buf[i + 0] = hex[self.bytes[j] >> 4];
            buf[i + 1] = hex[self.bytes[j] & 0x0f];
        }
        return buf;
    }

    // Indices in the UUID string representation for each byte.
    const encoded_pos = [16]u8{ 0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34 };

    // Hex
    const hex = "0123456789abcdef";

    // Hex to nibble mapping.
    const hex_to_nibble = [256]u8{
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    };

    pub fn format(
        self: UUID,
        comptime layout: []const u8,
        options: fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = options; // currently unused

        if (layout.len != 0 and layout[0] != 's')
            @compileError("Unsupported format specifier for UUID type: '" ++ layout ++ "'.");

        const buf = format_uuid(self);
        try fmt.format(writer, "{s}", .{buf});
    }

    pub fn parse(buf: []const u8) Error!UUID {
        var uuid = UUID{ .bytes = undefined };

        if (buf.len != 36 or buf[8] != '-' or buf[13] != '-' or buf[18] != '-' or buf[23] != '-')
            return Error.InvalidUUID;

        inline for (encoded_pos, 0..) |i, j| {
            const hi = hex_to_nibble[buf[i + 0]];
            const lo = hex_to_nibble[buf[i + 1]];
            if (hi == 0xff or lo == 0xff) {
                return Error.InvalidUUID;
            }
            uuid.bytes[j] = hi << 4 | lo;
        }

        return uuid;
    }
};

// Zero UUID
pub const zero: UUID = .{ .bytes = .{0} ** 16 };

// Convenience function to return a new v4 UUID.
pub fn newV4() UUID {
    return UUID.init();
}
