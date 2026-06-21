// Compile the shared echo.proto into prost-generated message types.
// Output lands in $OUT_DIR (under target/), so it's already excluded
// from git by Cargo's standard .gitignore.

fn main() {
    let proto = "../../proto/echo.proto";
    println!("cargo:rerun-if-changed={proto}");
    prost_build::Config::new()
        .compile_protos(&[proto], &["../../proto/"])
        .expect("Failed to compile echo.proto");
}
