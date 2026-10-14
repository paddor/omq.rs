#![cfg(all(feature = "soak", feature = "lz4"))]
//! Sustained, fully verified compression arenas and large-message offloads.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

#[path = "soak_common/compression.rs"]
mod compression;
mod soak_common;

#[test]
fn soak_compression_lz4_sustained() {
    compression::run(omq_tokio::CompressionKind::Lz4);
}
