//! Core
//!
//! This is the core module for Clickhouse

pub mod fmt;
mod tbl;
mod ty;
mod val;

pub use tbl::*;
pub use ty::*;
pub use val::*;

#[cfg(feature = "time")]
pub mod time;

#[cfg(feature = "uuid")]
pub mod uuid;

#[cfg(test)]
mod tests;
