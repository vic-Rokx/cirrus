const std = @import("std");

const Node = struct {
    value: []const u8,
    next: ?*Node,
    prev: ?*Node,
};

fn newNode(allocator: std.mem.Allocator, value: []const u8) !*Node {
    const node = try allocator.create(Node);
    node.* = Node{
        .value = value,
        .next = null,
        .prev = null,
    };
    return node;
}

pub const DLinkedList = struct {
    const Self = @This();

    head: ?*Node,
    tail: ?*Node,
    current_node: ?*Node,
    allocator: std.mem.Allocator,
    size: usize,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .head = null,
            .tail = null,
            .current_node = null,
            .allocator = allocator,
            .size = 0,
        };
    }

    pub fn addFront(self: *Self, value: []const u8) !void {
        var new_node = try newNode(self.allocator, value);

        if (self.head != null) {
            new_node.next = self.head;
            self.head.?.prev = new_node;
        } else {
            self.tail = new_node;
        }

        self.head = new_node;
        self.current_node = new_node;
        self.size += 1;
    }
    pub fn next(self: *Self) ?*Node {
        if (self.current_node == null) {
            self.current_node = self.head.?;
            return self.current_node.?;
        } else {
            self.current_node = self.current_node.?.next;
            return self.current_node;
        }
    }

    pub fn addBack(self: *Self, value: []const u8) !void {
        var new_node = try newNode(self.allocator, value);
        if (self.tail != null) {
            new_node.prev = self.tail;
            self.tail.?.next = new_node;
        } else {
            self.head = new_node;
            self.current_node = new_node;
        }
        self.size += 1;
        self.tail = new_node;
    }

    pub fn removeFront(self: *Self) !void {
        if (self.head == null) return;
        const old_head = self.head;

        self.head = old_head.?.next;
        if (self.head != null) {
            self.head.?.prev = null;
        } else {
            self.tail = null;
        }

        self.size -= 1;
        self.allocator.destroy(old_head);
    }

    pub fn removeBack(self: *Self) !void {
        if (self.tail == null) return;
        const old_tail = self.tail;
        self.tail = old_tail.?.prev;

        if (self.tail != null) {
            self.tail.?.next = null;
        } else {
            self.head = null;
        }

        self.size -= 1;
        self.allocator.destroy(old_tail);
    }
};

// test "Create linkedList" {
//     var ll = DLinkedList.init(std.heap.page_allocator);
//     try ll.addFront("Node Two");
//     try ll.addFront("Node One");
//     try ll.addBack("Node Five");
//
//     _ = ll.next();
//     _ = ll.next();
//     const curr_node = ll.next();
//     std.debug.print("\n This is the current node{s}\n", .{curr_node.?.value});
// }
