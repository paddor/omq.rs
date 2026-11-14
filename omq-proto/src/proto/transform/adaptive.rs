//! Sender bottleneck of one Zstd adaptation window.
//!
//! zrip's `AdaptiveLevel` walks the levels. The connection driver measures
//! each window, and this module names what limited the sender.

use zrip::adaptive::Bottleneck;

use super::OutputWindow;

/// What clearly limited the sender during `output`, if anything.
pub(super) fn bottleneck(output: OutputWindow) -> Option<Bottleneck> {
    let elapsed = output.elapsed.as_nanos();
    let blocked = output.blocked.as_nanos();

    if elapsed == 0 || output.batches == 0 {
        return None;
    }

    // The transport refused bytes for at least 1/16 of the window.
    if blocked > 0 && blocked * 16 >= elapsed {
        return Some(Bottleneck::Output);
    }

    // The input queue filled up, or more than 1/8 of the batches stopped at
    // their limit with input left. A shorter backlog only grows the batches.
    if output.input_full > 0 || u64::from(output.overflowed) * 8 > u64::from(output.batches) {
        return (blocked * 64 < elapsed).then_some(Bottleneck::Cpu);
    }

    (output.backlogged == 0).then_some(Bottleneck::Input)
}

#[cfg(test)]
mod tests;
