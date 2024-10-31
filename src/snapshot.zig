const std = @import("std");
const Entry = @import("cache.zig").Entry;
const Cache = @import("cache.zig");
const Types = @import("types.zig");
const DLL = @import("storage/dll.zig").DLinkedList;

pub const SnapShotError = error{
    FailedToLoadFile,
    FailedToReadFile,
    FailedToSnapCache,
    FailedToWriteToDisk,
};

const Self = @This();
arena: *std.mem.Allocator,

pub fn init(snapshot: *Self, gpa: *std.mem.Allocator) void {
    snapshot.* = .{
        .arena = gpa,
    };
}

fn diskWriter(writer: *std.fs.File.Writer, slice: []const u8) SnapShotError!void {
    _ = writer.*.write(slice) catch {
        return SnapShotError.FailedToWriteToDisk;
    };
}

fn saveToFile(filename: []const u8, cache: *Cache) !void {
    const map = cache.*.map;
    var file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    var writer = file.writer();
    var hash_map_itr = map.iterator();
    while (hash_map_itr.next()) |entry| {
        try diskWriter(&writer, "key:");
        try diskWriter(&writer, entry.key_ptr.*);
        try diskWriter(&writer, ";\n");
        try diskWriter(&writer, "value:");
        const type_resp = entry.value_ptr.value;
        switch (type_resp) {
            .dll => |v| {
                var node = v.*.head;
                try diskWriter(&writer, "dll:");
                for (0..v.size) |i| {
                    if (node != null) {
                        try diskWriter(&writer, node.?.*.value);
                        node = node.?.*.next;
                        if (i == v.size - 1) continue;
                        try diskWriter(&writer, ":");
                    }
                }
            },
            .array => |v| {
                try diskWriter(&writer, "array:");
                var buf_size: [256]u8 = undefined;
                const array_size_str = try std.fmt.bufPrint(&buf_size, "{}", .{v.values.len});
                try diskWriter(&writer, array_size_str);
                try diskWriter(&writer, ":");
                for (v.values, 0..) |resp, i| {
                    switch (resp) {
                        .dll => {},
                        .array => {},
                        .int => |iv| {
                            try diskWriter(&writer, "int:");
                            var buf: [256]u8 = undefined;
                            const str = try std.fmt.bufPrint(&buf, "{}", .{iv});
                            try diskWriter(&writer, str);
                            // try writer.writeInt(i32, iv, std.builtin.Endian.little);
                        },
                        .string => |iv| {
                            try diskWriter(&writer, "string:");
                            try diskWriter(&writer, iv);
                        },
                    }
                    if (i == v.values.len - 1) continue;
                    try diskWriter(&writer, ":");
                }
            },
            .int => |v| {
                try diskWriter(&writer, "int:");
                var buf: [256]u8 = undefined;
                const str = try std.fmt.bufPrint(&buf, "{}", .{v});
                try diskWriter(&writer, str);
                // try writer.writeInt(u32, 22, std.builtin.Endian.little);
            },
            .string => |v| {
                try diskWriter(&writer, "string:");
                try diskWriter(&writer, v);
            },
        }
        try diskWriter(&writer, ";\n");
    }
    try diskWriter(&writer, ";\n");
}

fn parseKey(key_line: []const u8) []const u8 {
    var key_line_itr = std.mem.splitScalar(u8, key_line, ':');
    _ = key_line_itr.next();

    const key = key_line_itr.next().?;
    return key;
}

const TypeEntry = enum {
    int,
    array,
    string,
    dll,
};

fn parseValue(self: *Self, value_line: []const u8) !Types.RESP {
    var entry: Types.RESP = undefined;
    var value_line_itr = std.mem.splitScalar(u8, value_line, ':');
    _ = value_line_itr.next();
    const value_type = value_line_itr.next().?;
    const value_type_enum = std.meta.stringToEnum(TypeEntry, value_type);
    switch (value_type_enum.?) {
        .int => {
            const value = try std.fmt.parseInt(i32, value_line_itr.next().?, 10);
            entry = Types.RESP{ .int = value };
        },
        .array => {
            const len: usize = @intCast(try std.fmt.parseInt(i32, value_line_itr.next().?, 10));
            const resp_array = Types.RESP{
                .array = .{
                    .arena = self.arena,
                    .values = try self.arena.*.alloc(Types.RESP, len),
                },
            };
            var idx: u16 = 0;
            var elem_type_enum: ?TypeEntry = null;
            while (value_line_itr.next()) |elem| {
                // Type
                elem_type_enum = std.meta.stringToEnum(TypeEntry, elem);
                // Value
                const value_buf = value_line_itr.next().?;
                switch (elem_type_enum.?) {
                    .int => {
                        const value = try std.fmt.parseInt(i32, value_buf, 10);
                        resp_array.array.values[idx] = Types.RESP{ .int = value };
                    },
                    .string => {
                        resp_array.array.values[idx] = Types.RESP{ .string = value_buf };
                    },
                    .array => {},
                    .dll => {},
                }
                idx += 1;
            }
            entry = resp_array;
        },
        .string => {
            const value = value_line_itr.next().?;
            entry = Types.RESP{ .string = value };
        },
        .dll => {
            const dll = try self.arena.*.create(DLL);
            dll.* = DLL.init(std.heap.page_allocator);
            while (value_line_itr.next()) |elem| {
                try dll.addBack(elem);
            }

            entry = Types.RESP{ .dll = dll };
        },
    }
    return entry;
}

pub fn loadCacheFromDisk(self: *Self, filename: []const u8, empty_cache: *Cache) !void {
    var file = std.fs.cwd().openFile(filename, .{}) catch {
        return SnapShotError.FailedToLoadFile;
    };
    defer file.close();

    var reader = file.reader();
    const stat = file.stat() catch {
        return SnapShotError.FailedToReadFile;
    };
    const buffer = try reader.readAllAlloc(self.arena.*, stat.size);
    var tokenizer = std.mem.tokenizeAny(u8, buffer, ";\n");
    var lineIndex: usize = 0;
    while (tokenizer.next()) |line| { // Key _ = parseKey(line);
        const key = parseKey(line);
        const value_line = tokenizer.next().?;
        // Value
        const value = self.parseValue(value_line) catch {
            return SnapShotError.FailedToReadFile;
        };

        try empty_cache.set(key, value);
        lineIndex += 1;
    }
}

pub fn snapCaches(caches: *[]*Cache) !void {
    var sleep: u64 = 5_000_000_000;
    while (true) {
        for (caches.*, 0..) |cache, i| {
            if (cache.key_change_count >= 1000) {
                sleep = 5_000_000_000;
                var buf_size: [256]u8 = undefined;
                const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_caches/snapped_cache_{}.dat", .{i});
                std.debug.print("\nSnapping the cache: {s}", .{cacheName});
                saveToFile(cacheName, cache) catch {
                    return SnapShotError.FailedToSnapCache;
                };
                cache.key_change_count = 0;
            } else if (cache.key_change_count >= 1) {
                sleep = 5_000_000_000;
                var buf_size: [256]u8 = undefined;
                const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_caches/snapped_cache_{}.dat", .{i});
                std.debug.print("\nSnapping the cache: {s}", .{cacheName});
                saveToFile(cacheName, cache) catch {
                    return SnapShotError.FailedToSnapCache;
                };
                cache.key_change_count = 0;
            }
        }
        std.time.sleep(sleep);
    }
}

test "write to disk" {
    var arena = std.heap.c_allocator;
    var snapshot: Self = undefined;
    snapshot.init(&arena);
    std.debug.print("\nInitializing cache...\n", .{});
    var cache: Cache = undefined;
    try cache.init(&arena, 0);

    std.debug.print("Inserting cache values...\n", .{});
    var dll = DLL.init(arena);
    try dll.addFront("Node Two");
    try dll.addFront("Node One");
    try dll.addBack("Node Five");
    const set_dll_value = Types.RESP{ .dll = &dll };

    var array = [_]Types.RESP{
        Types.RESP{ .string = "Elem1" },
        Types.RESP{ .int = 324 },
        Types.RESP{ .string = "Elem3" },
        Types.RESP{ .string = "Elem4" },
    };
    const set_array_value = Types.RESP{ .array = .{
        .values = &array,
        .arena = &arena,
    } };

    const set_string_value = Types.RESP{ .string = "Vic" };
    const set_int_value = Types.RESP{ .int = 22 };
    try cache.set("name", set_string_value);
    try cache.set("list", set_array_value);
    try cache.set("age", set_int_value);
    try cache.set("dll", set_dll_value);
    std.debug.print("\n---SAVING CACHE TO DISK---\n", .{});
    try saveToFile("cache_one_disk.dat", &cache);
    std.debug.print("\nDeinitializing cache...\n", .{});
    cache.deinit();

    var empty_cache: Cache = undefined;
    try empty_cache.init(&arena, 0);
    std.debug.print("\nInitializing empty_cache...\n", .{});

    std.debug.print("LOADING CACHE FROM DISK...\n", .{});
    _ = try snapshot.loadCacheFromDisk("cache_one_disk.dat", &empty_cache);
    std.debug.print("\n\n---LOADED CACHE FROM DISK---\n", .{});
    std.debug.print("\n---READY TO USE---\n", .{});

    const get_string_value = empty_cache.get("name").?.value;
    const get_array_value = empty_cache.get("list").?.value;
    const get_int_value = empty_cache.get("age").?.value;
    // const get_dll_value = empty_cache.get("dll").?.value;

    try std.testing.expectEqualDeep(set_array_value, get_array_value);
    try std.testing.expectEqualDeep(get_string_value, set_string_value);
    try std.testing.expectEqualDeep(get_int_value, set_int_value);
    // try std.testing.expectEqualDeep(get_dll_value, set_dll_value);

    std.debug.print("\n", .{});
}
