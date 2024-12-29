fn main() {
    // println!("cargo:rerun-if-changed=src/ffi/local_file_system.rs");
    // println!("cargo:rerun-if-changed=src/ffi/sqlite.rs");
    let _build = cxx_build::bridges(vec![
        "src/ffi/local_file_system.rs",
        "src/ffi/memory_cache.rs",
        "src/ffi/sqlite.rs",
        "src/ffi/memmap.rs",
        "src/ffi/azure_trace.rs",
    ]);
}
