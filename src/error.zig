const std = @import("std");

pub fn assert_cm(ok: bool, error_msg: []const u8) void {
    if (!ok) {
        const boldRed = "\x1b[1;31m"; // ANSI escape code for bold + red
        const reset = "\x1b[0m"; // Reset ANSI code to clear formatting
        std.debug.print("\n{s}Error{s}: ", .{ boldRed, reset });
        std.debug.print("{s}\n", .{error_msg});
        unreachable; // assertion failure
    }
}
