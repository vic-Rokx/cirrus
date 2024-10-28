const std = @import("std");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Load your TLS certificate and private key from files.
    const cert_pem = try readFile("server.crt", allocator);
    const key_pem = try readFile("server.key", allocator);
    defer allocator.free(cert_pem);
    defer allocator.free(key_pem);

    // Create a TLS context with the certificate and key.
    // const tls_config = try std.crypto.tls.ServerConfig.init(cert_pem, key_pem);
    var ca_bundle: std.crypto.Certificate.Bundle = .{};
    ca_bundle.rescan(allocator) catch return error.CertificateBundleLoadFailure;
    const stream = try std.net.tcpConnectToHost(allocator, "127.0.0.1", 8081);
    var tls_client = try std.crypto.tls.Client.init(stream, ca_bundle, "127.0.0.1");
    try tls_client.writeAll(stream, "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
    // std.crypto.Certificate.Bundle;
    // defer tls_config.deinit();
    //
    //
    // std.debug.print("TLS server listening on port 4433...\n", .{});
    //
    // while (true) {
    //     const conn = try listener.acceptStream();
    //     defer conn.deinit();
    //
    //     const tls_stream = try tls_config.accept(conn);
    //     defer tls_stream.deinit();
    //
    //     const buffer: [1024]u8 = undefined;
    //     const bytes_read = try tls_stream.reader().readAll(&buffer);
    //     std.debug.print("Received: {s}\n", .{buffer[0..bytes_read]});
    //
    //     const response = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\nHello, TLS!";
    //     try tls_stream.writer().writeAll(response);
    //
    //     std.debug.print("Response sent.\n", .{});
    // }
}

// Utility function to read a file into memory.
fn readFile(filename: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();
    return try file.readToEndAlloc(allocator, std.math.maxInt(usize));
}
