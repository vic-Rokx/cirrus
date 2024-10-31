const std = @import("std");
const posix = std.posix;
// const utils = @import("../utils/index.zig");

const ClientError = error{
    HeaderMalformed,
    RequestNotSupported,
    ProtoNotSupported,
    Success,
    ValueNotFound,
    FailedToSet,
    IndexOutOfBounds,
    ConnectionRefused,
    ServerError,
};

const ReturnTypes = enum {
    Success,
};

const Conn = struct {
    fd: c_int,
};

const Self = @This();
client_addr: std.net.Address,
pub fn createClient(port: u16) !Self {
    const client_addr = try std.net.Address.parseIp4("127.0.0.1", port);
    return Self{
        .client_addr = client_addr,
    };
}

fn createConn(self: Self) !c_int {
    const client_fd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    var option_value: i32 = 1; // Enable the option
    const option_value_bytes = std.mem.asBytes(&option_value);
    try posix.setsockopt(client_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, option_value_bytes);
    posix.connect(client_fd, &self.client_addr.any, self.client_addr.getOsSockLen()) catch |err| {
        if (err == error.ConnectionRefused) {
            // utils.error_print_str("Error: Cache connection not available");
            return ClientError.ConnectionRefused;
        } else {
            // utils.error_print_str("Error: Cache Internal server error");
            return ClientError.ServerError;
        }
    };
    return client_fd;
}

pub fn close(self: Self) void {
    posix.close(self.client_fd);
}

pub fn ping(self: Self) ![]const u8 {
    const client_fd = try self.createConn();
    std.debug.print("Nimbus client fd: {d}\n", .{client_fd});
    const nw = try posix.write(client_fd, "$4\r\nPING\r\n");
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);

    const resp = rbuf[0..nr];
    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.FailedToSet;
    }

    return "PONG";
}

pub fn echo(self: Self, value: []const u8) ![]const u8 {
    const client_fd = try self.createConn();
    defer posix.close(client_fd);
    const req = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*2\r\n$4\r\nECHO\r\n${d}\r\n{s}\r\n",
        .{ value.len, value },
    );

    const nw = try posix.write(client_fd, req);
    if (nw < 0) {
        return ClientError.RequestNotSupported;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);

    const resp = rbuf[0..nr];
    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.FailedToSet;
    }
    std.debug.print("\nserver response: {s}", .{rbuf[0..nr]});
    const s = try std.heap.c_allocator.alloc(u8, nr - 4);
    std.mem.copyForwards(u8, s, rbuf[4..nr]);
    return s;
}

pub fn set(self: Self, key: []const u8, value: []const u8) ![]const u8 {
    const client_fd = try self.createConn();
    const response = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*3\r\n$3\r\nSET\r\n${d}\r\n{s}\r\n${d}\r\n{s}\r\n",
        .{ key.len, key, value.len, value },
    );

    const nw = try posix.write(client_fd, response);
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);

    const resp = rbuf[0..nr];
    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.FailedToSet;
    }

    const s = try std.heap.c_allocator.alloc(u8, nr);
    std.mem.copyForwards(u8, s, rbuf[0..nr]);
    return s;
}

pub fn get(self: Self, key: []const u8) ![]const u8 {
    const client_fd = try self.createConn();
    const response = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*2\r\n$3\r\nGET\r\n${d}\r\n{s}\r\n",
        .{ key.len, key },
    );

    const nw = try posix.write(client_fd, response);
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);
    const resp = rbuf[0..nr];

    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.ValueNotFound;
    }

    const s = try std.heap.c_allocator.alloc(u8, nr - 4);
    std.mem.copyForwards(u8, s, rbuf[4..nr]);
    return s;
}

pub fn del(self: Self, key: []const u8) ![]const u8 {
    const client_fd = try self.createConn();
    const response = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*2\r\n$3\r\nDEL\r\n${d}\r\n{s}\r\n",
        .{ key.len, key },
    );

    const nw = try posix.write(client_fd, response);
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);
    const resp = rbuf[0..nr];

    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.ValueNotFound;
    }

    const s = try std.heap.c_allocator.alloc(u8, nr);
    std.mem.copyForwards(u8, s, rbuf[0..nr]);
    return s;
}

// "*3\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$3\r\none\r\n";
// *4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$4\r\nfive\r\n$3\r\nsix\r\n
pub fn lpush(self: Self, comptime num_items: usize, llname: []const u8, items: []const []const u8) ![]const u8 {
    const client_fd = try self.createConn();
    const precursor = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*{d}\r\n$5\r\nLPUSH\r\n${d}\r\n{s}\r\n",
        .{ items.len + 2, llname.len, llname },
    );

    const str_arr = items;
    var input: []u8 = undefined;
    var str_arr_v: [num_items][]const u8 = undefined;
    for (str_arr, 0..) |v, i| {
        const response = try std.fmt.allocPrint(
            std.heap.c_allocator,
            "${d}\r\n{s}\r\n",
            .{ v.len, v },
        );

        str_arr_v[i] = response;
    }
    input = try std.mem.join(std.heap.c_allocator, "", &str_arr_v);
    const final = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "{s}{s}",
        .{ precursor, input },
    );

    // _ = try posix.write(self.client_fd, "*5\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$4\r\nfive\r\n$3\r\nsix\r\n$4\r\nfour\r\n");
    const nw = try posix.write(client_fd, final);
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);
    const resp = rbuf[0..nr];

    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.ValueNotFound;
    }

    const s = try std.heap.c_allocator.alloc(u8, nr - 1);
    std.mem.copyForwards(u8, s, rbuf[1..nr]);
    return s;
}

// "*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n"
pub fn lrange(self: Self, ll_name: []const u8, start: []const u8, end: []const u8) !void {
    const client_fd = try self.createConn();
    const req = try std.fmt.allocPrint(
        std.heap.c_allocator,
        "*4\r\n$6\r\nLRANGE\r\n${d}\r\n{s}\r\n+{d}\r\n{s}\r\n+{d}\r\n{s}\r\n",
        .{ ll_name.len, ll_name, start.len, start, end.len, end },
    );
    const nw = try posix.write(client_fd, req);
    if (nw < 0) {
        return;
    }
    var rbuf: [1024]u8 = undefined;
    const nr = try posix.read(client_fd, &rbuf);
    const resp = rbuf[0..nr];

    if (std.mem.eql(u8, resp, "-ERROR INDEX RANGE")) {
        return ClientError.IndexOutOfBounds;
    }

    if (std.mem.eql(u8, resp, "-ERROR")) {
        return ClientError.ValueNotFound;
    }

    std.debug.print("\nresponse: {s}", .{rbuf[0..nr]});
}
