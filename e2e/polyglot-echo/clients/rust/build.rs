fn main() {
    let protos = [
        "../../proto/echo.proto",
        "../../proto/echo_stream.proto",
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    prost_build::Config::new()
        .compile_protos(&protos, &["../../proto/"])
        .expect("Failed to compile proto files");
}
