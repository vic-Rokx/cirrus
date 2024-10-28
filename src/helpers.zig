const std = @import("std");

pub fn stringPrint(preq: []const u8, value: []const u8) void {
    std.debug.print("\n{s}: {s}\n", .{ preq, value });
}
