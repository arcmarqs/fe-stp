@0x94a43df6c359e805;

using Rust = import "rust.capnp";
$Rust.parentModule("serialize");

struct Request {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    action      @2 :Data;
}

struct Reply {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    data        @2 :Data;
}