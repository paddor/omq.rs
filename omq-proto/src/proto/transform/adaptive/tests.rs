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

/// The 1/8 share is computed without overflowing the batch counter.
#[test]
fn a_huge_overflow_count_still_names_the_cpu() {
    let busy = OutputWindow {
        batches: u32::MAX,
        ..window(0, 64, u32::MAX, 0)
    };
    assert_eq!(bottleneck(busy), Some(Bottleneck::Cpu));
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
