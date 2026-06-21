fn main() {
    println!("cargo:rustc-check-cfg=cfg(actr_has_generated)");
    println!("cargo:rerun-if-changed=src/generated");

    if std::path::Path::new("src/generated/mod.rs").exists() {
        println!("cargo:rustc-cfg=actr_has_generated");
    }
}
