const MESSAGE_CAPNP_SRC: &str = "src/schemas/messages.capnp";

fn main() {
    // recompile capnp message into rust when the source changes
    println!("cargo:rerun-if-changed={}", MESSAGE_CAPNP_SRC);
    capnpc::CompilerCommand::new()
        .src_prefix("src/schemas")
        .file(MESSAGE_CAPNP_SRC)
        .run()
        .unwrap();
}