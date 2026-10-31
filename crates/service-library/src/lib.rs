//! The library of a Celaut service.
//!
//! A service runs on a node, and the node tells it where its gateway is
//! ([`config::NodeConfig`]). The service uses the gateway as an *instance*: it needs no
//! `Client`, and the token of each instance that it starts is the credential for it
//! ([`gateway::Gateway`]). [`manager::DependencyManager`] keeps those instances in a pool,
//! so a service starts a dependency once, reuses it, and stops it when it is idle or
//! broken.
//!
//! This is the Rust version of `celaut-service-libraries` (Python). What a service does
//! not need is not here: there is no balancing between nodes, because the node of the
//! service does that.

pub mod config;
pub mod counter;
pub mod error;
pub mod gateway;
pub mod instance;
pub mod manager;
pub mod package;
pub mod receipt;
pub mod relay;
pub mod skips;
pub mod tunnel;

pub use error::Error;
