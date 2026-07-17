use std::env;
use std::path::{Path, PathBuf};

fn verify_ffi_guards(src_dir: &Path) {
    for entry in std::fs::read_dir(src_dir).expect("failed to read rust/src") {
        let path = entry.expect("failed to read rust/src entry").path();
        if path.is_dir() {
            verify_ffi_guards(&path);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let source = std::fs::read_to_string(&path).expect("failed to read Rust source");
        let lines: Vec<_> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains("#[unsafe(no_mangle)]") {
                continue;
            }
            let guarded = lines[..index]
                .iter()
                .rev()
                .find(|candidate| !candidate.trim().is_empty())
                .is_some_and(|candidate| candidate.contains("lance_go_ffi_macros::ffi_guard"));
            assert!(
                guarded,
                "{}:{}: every no_mangle export must have an immediately preceding ffi_guard",
                path.display(),
                index + 1
            );
        }
    }
}

fn main() {
    let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    verify_ffi_guards(&crate_dir.join("src"));

    let config = cbindgen::Config::from_file(crate_dir.join("cbindgen.toml"))
        .expect("failed to read cbindgen.toml");

    let header_path = crate_dir.join("..").join("include").join("lance_go.h");
    std::fs::create_dir_all(header_path.parent().unwrap())
        .expect("failed to create include/ directory");

    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .expect("cbindgen failed to generate bindings")
        .write_to_file(&header_path);

    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
