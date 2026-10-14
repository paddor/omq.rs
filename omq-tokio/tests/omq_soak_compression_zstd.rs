#![cfg(all(feature = "soak", feature = "zstd"))]
//! Sustained, fully verified compression arenas and large-message offloads.

#[global_allocator]
static GLOBAL: soak_common::alloc::TrackingAllocator = soak_common::alloc::TrackingAllocator;

#[path = "soak_common/compression.rs"]
mod compression;
mod soak_common;

#[test]
fn soak_compression_zstd_sustained() {
    compression::run(omq_tokio::CompressionKind::Zstd);
}
