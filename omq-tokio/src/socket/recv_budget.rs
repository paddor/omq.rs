//! Async receives spend the calling task's Tokio budget.

/// Spend one unit of the calling task's Tokio budget, yielding first once it
/// is spent, like a Tokio channel receive. A loop over a socket that stays
/// ready then lets the other tasks and drivers on its worker run. Outside a
/// Tokio task there is no budget.
#[inline]
pub(super) async fn cooperate() {
    tokio::task::coop::consume_budget().await;
}
