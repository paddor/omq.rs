//! Optional transport diagnostics for native async and blocking sockets.

use crate::{DartCapabilities, DartStats, Socket};

/// Approximate DART counters across a socket's endpoints.
///
/// Counters remain available after endpoint closure and are zero for unused
/// DART transports. The snapshot is not atomic across fields. Missing ECN
/// metadata is counted separately from Not-ECT.
pub fn dart_stats(socket: &impl AsRef<Socket>) -> DartStats {
    socket.as_ref().dart_state().counters.snapshot()
}

/// Conservative UDP offload and ECN capabilities across live DART endpoints.
///
/// Returns `None` when no DART endpoint is live. Reading capabilities does not
/// retain endpoints or extend their lifetime.
pub fn dart_capabilities(socket: &impl AsRef<Socket>) -> Option<DartCapabilities> {
    socket.as_ref().dart_state().capabilities()
}
