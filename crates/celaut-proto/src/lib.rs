//! Protobuf types of the Celaut node `Gateway`.
//!
//! The `.proto` files in `protos/` are copied from
//! [celaut-project/nodo](https://github.com/celaut-project/nodo) (`protos/`). The commit
//! is in `protos/SOURCE`. The Rust code in `src/gen/` is generated from them with
//! `prost-build` and committed, so a build needs no `protoc`.

#![allow(clippy::all, missing_docs)]

pub mod buffer {
    include!("gen/buffer.rs");
}

pub mod celaut {
    include!("gen/celaut.rs");
}

pub use prost::Message;
