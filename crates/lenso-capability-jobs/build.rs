use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=capability.json");
    println!("cargo:rerun-if-changed=schemas");
    println!("cargo:rerun-if-changed=src/generated.rs");
    println!("cargo:rerun-if-changed=generated/bindings.ts");
    lenso_contract_codegen::check_generated(
        Path::new("capability.json"),
        Path::new("src/generated.rs"),
        Path::new("generated/bindings.ts"),
    )
    .expect("generated Jobs Capability artifacts are stale");
}
