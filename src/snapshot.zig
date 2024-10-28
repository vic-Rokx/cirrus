const std = @import("std");
const Entry = @import("cache.zig").Entry;
const Cache = @import("cache.zig");
const Types = @import("types.zig");

fn saveToFile(filename: []const u8, cache: *Cache) !void {
    const map = cache.*.map;
    var file = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    var writer = file.writer();
    var hash_map_itr = map.iterator();
    while (hash_map_itr.next()) |entry| {
        _ = try writer.write("key:");
        _ = try writer.write(entry.key_ptr.*);
        _ = try writer.write(";\n");
        _ = try writer.write("value:");
        const type_resp = entry.value_ptr.value;
        switch (type_resp) {
            .dll => {},
            .array => |v| {
                _ = try writer.write("array:");
                var buf_size: [256]u8 = undefined;
                const array_size_str = try std.fmt.bufPrint(&buf_size, "{}", .{v.values.len});
                _ = try writer.write(array_size_str);
                _ = try writer.write(":");
                for (v.values, 0..) |resp, i| {
                    switch (resp) {
                        .dll => {},
                        .array => {},
                        .int => |iv| {
                            _ = try writer.write("int:");
                            var buf: [256]u8 = undefined;
                            const str = try std.fmt.bufPrint(&buf, "{}", .{iv});
                            _ = try writer.write(str);
                            // try writer.writeInt(i32, iv, std.builtin.Endian.little);
                        },
                        .string => |iv| {
                            _ = try writer.write("string:");
                            _ = try writer.write(iv);
                        },
                    }
                    if (i == v.values.len - 1) continue;
                    _ = try writer.write(":");
                }
            },
            .int => |v| {
                _ = try writer.write("int:");
                var buf: [256]u8 = undefined;
                const str = try std.fmt.bufPrint(&buf, "{}", .{v});
                _ = try writer.write(str);
                // try writer.writeInt(u32, 22, std.builtin.Endian.little);
            },
            .string => |v| {
                _ = try writer.write("string:");
                _ = try writer.write(v);
            },
        }
        _ = try writer.write(";\n");
    }
    _ = try writer.write(";\n");
}

fn parseKey(key_line: []const u8) []const u8 {
    var key_line_itr = std.mem.splitScalar(u8, key_line, ':');
    _ = key_line_itr.next();

    const key = key_line_itr.next().?;
    std.debug.print("\nkey:{s}", .{key});
    return key;
}

const TypeEntry = enum {
    int,
    array,
    string,
};

fn parseValue(value_line: []const u8) !Types.RESP {
    var entry: Types.RESP = undefined;
    var value_line_itr = std.mem.splitScalar(u8, value_line, ':');
    _ = value_line_itr.next();
    const value_type = value_line_itr.next().?;
    const value_type_enum = std.meta.stringToEnum(TypeEntry, value_type);
    switch (value_type_enum.?) {
        .int => {
            const value = try std.fmt.parseInt(i32, value_line_itr.next().?, 10);
            std.debug.print("\nvalue_int:{d}", .{value});
            entry = Types.RESP{ .int = value };
        },
        .array => {
            const len: usize = @intCast(try std.fmt.parseInt(i32, value_line_itr.next().?, 10));
            const resp_array = Types.RESP{
                .array = .{
                    .allocator = std.heap.c_allocator,
                    .values = try std.heap.c_allocator.alloc(Types.RESP, len),
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
                        std.debug.print("\nvalue_type:{s}", .{elem});
                        std.debug.print("\nvalue_type:{s}", .{elem});
                        const value = try std.fmt.parseInt(i32, value_buf, 10);
                        std.debug.print("\nvalue_int:{d}", .{value});
                        resp_array.array.values[idx] = Types.RESP{ .int = value };
                    },
                    .string => {
                        std.debug.print("\nvalue_string:{s}", .{
                            elem,
                        });
                        resp_array.array.values[idx] = Types.RESP{ .string = value_buf };
                    },
                    .array => {},
                }
                idx += 1;
            }
            entry = resp_array;
        },
        .string => {
            const value = value_line_itr.next().?;
            std.debug.print("\nvalue_string:{s}", .{value});
            entry = Types.RESP{ .string = value };
        },
    }
    return entry;
}

fn loadFromFile(filename: []const u8, empty_cache: *Cache) !void {
    var file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var reader = file.reader();
    const stat = try file.stat();
    const buffer = try reader.readAllAlloc(std.heap.c_allocator, stat.size);
    var tokenizer = std.mem.tokenizeAny(u8, buffer, ";\n");
    var lineIndex: usize = 0;
    while (tokenizer.next()) |line| { // Key _ = parseKey(line);
        const key = parseKey(line);
        const value_line = tokenizer.next().?;
        // Value
        const value = try parseValue(value_line);
        try empty_cache.set(key, value);
        lineIndex += 1;
    }
}

fn snap(cacheName: []const u8, cache: *Cache) !void {
    while (true) {
        std.debug.print("\nSnapping the cache", .{cacheName});
        try saveToFile(cacheName, cache);
        std.time.sleep(60_000_000_000);
    }
}

pub fn snapCaches(caches: *[]*Cache) !void {
    var sleep: u64 = 60_000_000_000;
    while (true) {
        for (caches.*, 0..) |cache, i| {
            if (cache.key_change_count >= 1000) {
                sleep = 60_000_000_000;
                var buf_size: [256]u8 = undefined;
                const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_cache_{}.dat", .{i});
                std.debug.print("\nSnapping the cache: {s}", .{cacheName});
                try saveToFile(cacheName, cache);
                cache.key_change_count = 0;
            } else if (cache.key_change_count >= 1) {
                sleep = 300_000_000_000;
                var buf_size: [256]u8 = undefined;
                const cacheName = try std.fmt.bufPrint(&buf_size, "snapped_cache_{}.dat", .{i});
                std.debug.print("\nSnapping the cache: {s}", .{cacheName});
                try saveToFile(cacheName, cache);
                cache.key_change_count = 0;
            }
        }
        std.time.sleep(sleep);
    }
}

test "write to disk" {
    std.debug.print("\nInitializing cache...\n", .{});
    var cache: Cache = undefined;
    var arena = std.heap.c_allocator;
    try cache.init(&arena, 0);

    std.debug.print("Inserting cache values...\n", .{});
    var array = [_]Types.RESP{
        Types.RESP{ .string = "Elem1" },
        Types.RESP{ .int = 324 },
        Types.RESP{ .string = "Elem3" },
        Types.RESP{ .string = "Elem4" },
    };
    const set_array_value = Types.RESP{ .array = .{
        .values = &array,
        .allocator = std.heap.c_allocator,
    } };
    const set_string_value = Types.RESP{ .string = "Vic" };
    const set_int_value = Types.RESP{ .int = 22 };
    try cache.set("name", set_string_value);
    try cache.set("list", set_array_value);
    try cache.set("age", set_int_value);
    std.debug.print("\n---SAVING CACHE TO DISK---\n", .{});
    try saveToFile("cache_one_disk.dat", &cache);
    std.debug.print("\nDeinitializing cache...\n", .{});
    cache.deinit();

    var empty_cache: Cache = undefined;
    try empty_cache.init(&arena, 0);
    std.debug.print("\nInitializing empty_cache...\n", .{});

    std.debug.print("LOADING CACHE FROM DISK...\n", .{});
    _ = try loadFromFile("cache_one_disk.dat", &empty_cache);
    std.debug.print("\n\n---LOADED CACHE FROM DISK---\n", .{});
    std.debug.print("\n---READY TO USE---\n", .{});

    const get_string_value = empty_cache.get("name").?.value;
    const get_array_value = empty_cache.get("list").?.value;
    const get_int_value = empty_cache.get("age").?.value;

    try std.testing.expectEqualDeep(set_array_value, get_array_value);
    try std.testing.expectEqualDeep(get_string_value, set_string_value);
    try std.testing.expectEqualDeep(get_int_value, set_int_value);

    std.debug.print("\n", .{});
}
