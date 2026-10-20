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
    if output.input_full > 0 || output.overflowed * 8 > output.batches {
        return (blocked * 64 < elapsed).then_some(Bottleneck::Cpu);
    }
    (output.backlogged == 0).then_some(Bottleneck::Input)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn window(blocked_ms: u64, backlogged: u32, overflowed: u32, input_full: u32) -> OutputWindow {
        OutputWindow {
            elapsed: Duration::from_millis(100),
            blocked: Duration::from_millis(blocked_ms),
            batches: 64,
            backlogged,
            overflowed,
            input_full,
        }
    }

    #[test]
    fn clear_windows_name_their_bottleneck() {
        assert_eq!(bottleneck(window(100, 64, 64, 1)), Some(Bottleneck::Output));
        assert_eq!(bottleneck(window(7, 0, 0, 0)), Some(Bottleneck::Output));
        assert_eq!(bottleneck(window(0, 64, 0, 1)), Some(Bottleneck::Cpu));
        assert_eq!(bottleneck(window(0, 64, 9, 0)), Some(Bottleneck::Cpu));
        assert_eq!(bottleneck(window(0, 0, 0, 0)), Some(Bottleneck::Input));
    }

    #[test]
    fn mixed_and_empty_windows_name_none() {
        // Input queued after every batch, but batches emptied the queue.
        assert_eq!(bottleneck(window(0, 64, 0, 0)), None);
        // Few batches stopped at their limit.
        assert_eq!(bottleneck(window(0, 64, 8, 0)), None);
        // Light blocking with a full queue is neither output- nor CPU-bound.
        assert_eq!(bottleneck(window(3, 64, 64, 1)), None);
        let empty = OutputWindow {
            batches: 0,
            ..window(0, 0, 0, 0)
        };
        assert_eq!(bottleneck(empty), None);
        let instant = OutputWindow {
            elapsed: Duration::ZERO,
            ..window(0, 0, 0, 0)
        };
        assert_eq!(bottleneck(instant), None);
    }
}
