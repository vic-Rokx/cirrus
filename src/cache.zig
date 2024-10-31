const std = @import("std");
const Types = @import("types.zig");
const Mutex = std.Thread.Mutex;

pub const Entry = struct { value: Types.RESP };
const Self = @This();

arena: *std.mem.Allocator,
map: std.StringHashMap(Entry),
replicas: std.ArrayList(*std.StringHashMap(Entry)),
key_change_count: u16,
lock: ?Mutex,

pub fn init(
    target: *Self,
    allocator: *std.mem.Allocator,
    replica_count: u16,
    enabled_multithread: bool,
) !void {
    var replicas: std.ArrayList(*std.StringHashMap(Entry)) = undefined;
    if (replica_count > 0) {
        replicas = std.ArrayList(*std.StringHashMap(Entry)).init(allocator.*);
        try initReplicas(allocator, replica_count, &replicas);
    } else {
        replicas = std.ArrayList(*std.StringHashMap(Entry)).init(allocator.*);
    }
    var lock: ?Mutex = null;
    if (enabled_multithread) lock = .{};
    target.* = .{
        .arena = allocator,
        .map = std.StringHashMap(Entry).init(allocator.*),
        .lock = lock,
        .replicas = replicas,
        .key_change_count = 0,
    };
}

fn initReplicas(
    allocator: *std.mem.Allocator,
    replicas_count: u16,
    replicas: *std.ArrayList(*std.StringHashMap(Entry)),
) !void {
    var idx: usize = 0;
    var cache_ptr: *std.StringHashMap(Entry) = undefined;
    while (idx < replicas_count) {
        cache_ptr = try allocator.*.create(std.StringHashMap(Entry));
        cache_ptr.* = std.StringHashMap(Entry).init(allocator.*);
        try replicas.*.append(cache_ptr);
        idx += 1;
    }

    const entry = Entry{ .value = Types.RESP{ .string = "samual" } };
    try replicas.*.items[0].put("name", entry);
}

pub fn set(self: *Self, key: []const u8, value: Types.RESP) !void {
    if (self.lock != null) {
        self.lock.?.lock();
        defer self.lock.?.unlock();
    }
    const entry = Entry{ .value = value };
    try self.map.put(key, entry);
    self.key_change_count += 1;
    if (self.replicas.items.len > 0) {
        for (self.replicas.items) |replica| {
            try replica.*.put(key, entry);
        }
    }
}

pub fn get(self: *Self, key: []const u8) ?Entry {
    if (self.lock != null) {
        self.lock.?.lock();
        defer self.lock.?.unlock();
    }

    return self.map.get(key);
}

pub fn del(self: *Self, key: []const u8) bool {
    if (self.lock != null) {
        self.lock.?.lock();
        defer self.lock.?.unlock();
    }

    const entry_exists = self.map.remove(key);
    for (self.replicas.items) |replica| {
        _ = replica.*.remove(key);
    }
    self.key_change_count += 1;
    return entry_exists;
}

pub fn deinit(self: *Self) void {
    for (self.replicas.items) |replica| {
        replica.deinit();
        self.arena.destroy(replica);
    }
    self.map.deinit();
    self.replicas.deinit();
}
