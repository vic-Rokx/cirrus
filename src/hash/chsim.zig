const std = @import("std");
const utils = @import("../utils.zig");
const Cache = @import("../cache.zig");

const HashRingError = error{
    NotValidKey,
};

pub const CacheNode = struct {
    id: []const u8,
    hash: u32,
    cache_ptr: *Cache,

    pub fn init(target: *CacheNode, cache_ptr: *Cache) !void {
        target.* = .{
            .id = undefined,
            .hash = undefined,
            .cache_ptr = cache_ptr,
        };
    }
};

pub const HashRing = struct {
    const Self = @This();
    hash_list: std.ArrayList(u32),
    hash_map: std.AutoHashMap(u32, CacheNode),
    servers: std.ArrayList(CacheNode),

    pub fn init(target: *Self, allocator: std.mem.Allocator) !void {
        target.* = .{
            .hash_list = std.ArrayList(u32).init(allocator),
            .hash_map = std.AutoHashMap(u32, CacheNode).init(allocator),
            .servers = std.ArrayList(CacheNode).init(allocator),
        };
    }

    pub fn deinit(self: *Self) !void {
        self.hash_map.deinit();
        self.hash_list.deinit();
        self.servers.deinit();
    }

    pub fn addNode(self: *Self, cache_ptr: *Cache) !void {
        var hash_buf: [36]u8 = undefined;
        _ = utils.newV4().to_string(&hash_buf);
        var cache_node: CacheNode = undefined;
        try cache_node.init(cache_ptr);
        const hash_id = utils.djbHash(&hash_buf);
        cache_node.id = &hash_buf;
        cache_node.hash = hash_id;
        try self.hash_map.put(hash_id, cache_node);
        try self.hash_list.append(hash_id);
        self.sortList();
    }

    pub fn addNodes(self: *Self, servers: *[]*Cache) !void {
        for (servers.*) |server| {
            try self.addNode(server);
        }
    }

    pub fn removeNode(self: *Self, hash_id: u32) HashRingError!void {
        const count = self.hash_map.count();
        const len = self.hash_list.items.len;
        utils.assert_cm(len > 0, "HashList is empty, cannot remove node");
        utils.assert_cm(count > 0, "HashMap is empty, cannot remove node");
        if (self.hash_map.remove(hash_id)) {
            var i: u32 = 0;
            while (i < self.hash_list.items.len) : (i += 1) {
                if (self.hash_list.items[i] == hash_id) {
                    _ = self.hash_list.popOrNull();
                }
            }
        } else {
            return HashRingError.NotValidKey;
        }
        utils.assert_cm(self.hash_list.items.len < len, "Node not removed HashList length is the same");
        utils.assert_cm(self.hash_map.count() < count, "Node not removed HashMap capacity is the same");
        self.sortList();
    }

    fn sortList(self: *Self) void {
        if (self.hash_list.items.len > 0) {
            const items = self.hash_list.items;
            var i: usize = 0;
            while (i < items.len - 1) : (i += 1) {
                var j: usize = 0;
                while (j < items.len - i - 1) : (j += 1) {
                    if (items[j] > items[j + 1]) {
                        const temp = items[j];
                        items[j] = items[j + 1];
                        items[j + 1] = temp;
                    }
                }
            }
        }
    }

    pub fn findNode(self: *Self, key: []const u8) ?CacheNode {
        const hash_value = utils.djbHash(key);
        std.debug.print("Finding node for key with hash value: {d:.2}\n", .{hash_value});

        var idx: u32 = 0;
        for (self.hash_list.items, 0..) |hash, i| {
            // std.debug.print("\n{d}:{d}", .{ hash, hash_value });
            if (hash >= hash_value) {
                idx = @intCast(i);
                return self.hash_map.get(self.hash_list.items[idx]);
            }
        }

        // If we've reached here, the hash_value is greater than all node positions,
        // so we wrap around to the first node
        if (self.hash_list.items.len > 0) {
            idx = 0;
            return self.hash_map.get(self.hash_list.items[idx]);
        }

        return null;
    }

    pub fn getNode(self: *Self, hash: u32) ?CacheNode {
        return self.hash_map.get(hash);
    }
};
