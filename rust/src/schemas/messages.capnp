@0x98f7e6f543a6619b;

using Rust = import "rust.capnp";
$Rust.parentModule("serialize");

struct BenchmarkRequest {
    data @0 :Data;
}

struct BenchmarkReply {
    data @0 :Data;
}