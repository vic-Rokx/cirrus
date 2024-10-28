const std = @import("std");
const Cirrus = @import("cirrus.zig");
const Cluster = @import("cluster.zig");

fn createNimbusCache(port: u16) !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var allocator = gpa.allocator();
    var nimbus: Cirrus = undefined;
    const config = Cirrus.Config{
        .addr = "127.0.0.1",
        .port = port,
        .allocator = &allocator,
        .replicas = 2,
    };

    try nimbus.init(config);
    try nimbus.run();
}

fn createCluster() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    // defer if (gpa.deinit() != .ok) @panic("Memmory leak...");
    var allocator = arena.allocator();

    const cache_configs = [_]Cluster.CacheConfig{
        Cluster.CacheConfig{
            .port = 7000,
            .address = "127.0.0.1",
        },
        Cluster.CacheConfig{
            .port = 7001,
            .address = "127.0.0.1",
        },
        Cluster.CacheConfig{
            .port = 7002,
            .address = "127.0.0.1",
        },
    };

    const config_cluster = Cluster.ClusterConfig{
        .cache_count = 3,
        .replica_count = 2,
        .cache_configs = &cache_configs,
        .gpa = &allocator,
        .cluster_host = "127.0.0.1",
        .cluster_port = 6379,
    };
    var cluster: Cluster = undefined;
    try cluster.init(config_cluster);
    try cluster.run();
    try cluster.deinit();
}

pub fn main() !void {
    try createCluster();
    // var thread0 = try std.Thread.spawn(.{}, createCluster, .{});
    // var thread1 = try std.Thread.spawn(.{}, createNimbusCache, .{6832});
    // var thread2 = try std.Thread.spawn(.{}, createNimbusCache, .{6383});
    // thread0.join();
    // thread1.join();
    // thread2.join();
}
