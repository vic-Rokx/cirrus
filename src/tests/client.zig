const std = @import("std");
const posix = std.posix;
const print = std.debug.print;
const NimbusClient = @import("./CreateClient.zig");

pub var cache_client_one: NimbusClient = undefined; // Nullable to track initialization status.
pub var cache_client_two: NimbusClient = undefined; // Nullable to track initialization status.

pub fn init() !void {
    // Initialize NimbusClient at runtime.
    const user_cache = try NimbusClient.createClient(6379);
    cache_client_one = user_cache;
    const dll_cache = try NimbusClient.createClient(6380);
    cache_client_two = dll_cache;
}

pub fn main() !void {
    try init();
    // _ = try cache_client_one.ping();
    // _ = try cache_client_two.ping();
    var str_arr = [_][]const u8{ "one", "two", "three" };
    _ = try cache_client_one.lpush(3, "mylist", &str_arr);
    // str_arr = [_][]const u8{ "four", "five", "six" };
    // _ = try cache_client_two.lpush(3, "mylist", &str_arr);
    const length = try cache_client_one.lrange("mylist", "0", "-1");
    std.debug.print("\n{s}", .{length[0..1]});

    // var client = try NimbusClient.createClient(6379);
    // _ = try client.ping();
    _ = try cache_client_one.set("name", "Vic");
    const value = try cache_client_one.get("name");
    std.debug.print("\n{s}", .{value});
    // var str_arr = [_][]const u8{ "one", "two", "three" };
    // _ = try client.lpush(3, "mylist", &str_arr);
    // _ = try client.lrange("mylist", "0", "-1");

    // var thread1 = try std.Thread.spawn(.{}, createProcess, .{});
    // var thread2 = try std.Thread.spawn(.{}, createProcess, .{});
    // thread1.join();
    // thread2.join();
    // try createProcess();
    // try createProcess();
    // var client = try CreateClient.createClient();
    // try trial(&client);
    // try trial1(&client);
}

fn trial(client: *NimbusClient) !void {
    var str_arr = [_][]const u8{ "one", "two", "three" };
    _ = try client.lpush(3, "mylist", &str_arr);
}

fn trial1(client: *NimbusClient) !void {
    _ = try client.lrange("mylist", "0", "-1");
}

test "test create cache and insert" {
    const cache_test_one = try NimbusClient.createClient(6379);
    var str_arr = [_][]const u8{ "one", "two", "three" };
    _ = try cache_test_one.lpush(3, "mylist", &str_arr);
    const length = try cache_test_one.lrange("mylist", "0", "-1");
    try std.testing.expect(std.mem.eql(u8, "3", length[0..1]));

    _ = try cache_test_one.set("name", "Vic");
    const value = try cache_test_one.get("name");
    try std.testing.expect(std.mem.eql(u8, "Vic", value));
}
