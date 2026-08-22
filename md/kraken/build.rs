//! Generates the Rust view of nlib's wire records from `common.h` itself, so
//! this crate publishes the C++ structs rather than a transcription of them.
//!
//! bindgen emits `#[repr(C)]` structs carrying compile-time size, alignment
//! and offset assertions, and no `Default` impl, so every record is built
//! from a struct literal naming every field: a field added to `common.h`
//! reaches this crate as a compile error instead of as frames nqbook drops
//! for the wrong size.

use std::path::PathBuf;

/// nlib is vendored as a submodule at the repository root.
const HEADER: &str = "../../third_party/nlib/include/nlib/common.h";

fn main() {
    println!("cargo::rerun-if-changed={HEADER}");
    println!("cargo::rerun-if-changed=build.rs");

    let out = PathBuf::from(std::env::var_os("OUT_DIR").expect("cargo sets OUT_DIR"));
    bindgen::Builder::default()
        .header(HEADER)
        .clang_args(["-x", "c++", "-std=c++23"])
        // The header's types live in `namespace nlib`, and `book` and
        // `metrics` belong to nqbook rather than to a publisher.
        .enable_cxx_namespaces()
        .allowlist_item("nlib::(order|trade|side|order_type|order_action|price_scale|qty_scale)")
        // `enum class` maps onto a Rust enum, so an action or a side is a
        // value the compiler checks rather than an integer code.
        .rustified_enum("nlib::(side|order_type|order_action)")
        .generate()
        .expect("generate nlib bindings")
        .write_to_file(out.join("nlib.rs"))
        .expect("write nlib bindings");
}
