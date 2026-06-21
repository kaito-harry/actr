fn main() {
    let proto = "../proto/echo_stream.proto";
    println!("cargo:rerun-if-changed={proto}");
    prost_build::Config::new()
        .compile_protos(&[proto], &["../proto/"])
        .expect("Failed to compile echo_stream.proto");
}
