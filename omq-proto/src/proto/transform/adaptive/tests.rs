use std::time::Duration;

use super::*;

fn window(blocked_ms: u64, overflowed: u32, input_full: u32) -> OutputWindow {
    OutputWindow {
        elapsed: Duration::from_millis(100),
        blocked: Duration::from_millis(blocked_ms),
        batches: 64,
        overflowed,
        input_full,
    }
}

#[test]
fn clear_windows_name_their_bottleneck() {
    assert_eq!(bottleneck(window(100, 64, 1)), Some(Bottleneck::Output));
    assert_eq!(bottleneck(window(7, 0, 0)), Some(Bottleneck::Output));
    assert_eq!(bottleneck(window(0, 0, 1)), Some(Bottleneck::Cpu));
    assert_eq!(bottleneck(window(0, 9, 0)), Some(Bottleneck::Cpu));
    assert_eq!(bottleneck(window(0, 0, 0)), Some(Bottleneck::Input));
}

#[test]
fn input_that_encoding_kept_up_with_asks_for_ratio() {
    // Input may queue behind each batch while no batch stops at its limit.
    assert_eq!(bottleneck(window(0, 0, 0)), Some(Bottleneck::Input));
    // Few batches stopped at their limit.
    assert_eq!(bottleneck(window(0, 8, 0)), Some(Bottleneck::Input));
    // Light blocking without a full queue.
    assert_eq!(bottleneck(window(3, 0, 0)), Some(Bottleneck::Input));
}

/// The 1/8 share is computed without overflowing the batch counter.
#[test]
fn a_huge_overflow_count_still_names_the_cpu() {
    let busy = OutputWindow {
        batches: u32::MAX,
        ..window(0, u32::MAX, 0)
    };
    assert_eq!(bottleneck(busy), Some(Bottleneck::Cpu));
}

#[test]
fn mixed_and_empty_windows_name_none() {
    // Light blocking with a full queue is neither output- nor CPU-bound.
    assert_eq!(bottleneck(window(3, 64, 1)), None);
    let empty = OutputWindow {
        batches: 0,
        ..window(0, 0, 0)
    };
    assert_eq!(bottleneck(empty), None);
    let instant = OutputWindow {
        elapsed: Duration::ZERO,
        ..window(0, 0, 0)
    };
    assert_eq!(bottleneck(instant), None);
}

fn stats(plain: u64, ratio_percent: u64) -> CompressStats {
    CompressStats {
        plain,
        compressed: plain * 100 / ratio_percent,
        busy: Duration::from_millis(50),
    }
}

fn kept_up() -> OutputWindow {
    window(0, 0, 0)
}

#[test]
fn a_climb_that_slows_the_sender_reverts() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), kept_up()), 1);
    // 1 compresses better but carries 17% less input.
    assert_eq!(walk.update(1, stats(15_000_000, 170), kept_up()), -1);
}

#[test]
fn small_losses_add_up_against_the_level_a_climb_left() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), kept_up()), 1);
    assert_eq!(walk.update(1, stats(17_500_000, 160), kept_up()), 2);
    // 2.8% less than at 1, but 5.6% less than at -1.
    assert_eq!(walk.update(2, stats(17_000_000, 165), kept_up()), 1);
}

#[test]
fn a_held_level_follows_the_input_rate() {
    let mut walk = LevelWalk::new(-7..=4, 4);
    assert_eq!(walk.update(4, stats(18_000_000, 156), kept_up()), 4);
    // Less input at the same level is no step to blame.
    assert_eq!(walk.update(4, stats(9_000_000, 156), kept_up()), 4);
}

#[test]
fn offloads_from_an_earlier_level_count_toward_the_rate() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), kept_up()), 1);
    walk.carry(2_000_000);
    assert_eq!(walk.carried(), 2_000_000);
    // 16 MB plus 2 MB carried is no slower: the step holds.
    assert_eq!(walk.update(1, stats(16_000_000, 170), kept_up()), 1);
    assert_eq!(walk.carried(), 0);
}

fn unclear() -> OutputWindow {
    OutputWindow {
        batches: 0,
        ..window(0, 0, 0)
    }
}

#[test]
fn a_held_level_that_falls_behind_a_lower_one_steps_down() {
    let mut walk = LevelWalk::new(-1..=1, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), kept_up()), 1);
    // Within 5% of -1, so the step holds.
    assert_eq!(walk.update(1, stats(17_500_000, 170), kept_up()), 1);
    assert_eq!(walk.update(1, stats(15_000_000, 170), kept_up()), -1);
}

#[test]
fn one_slow_window_does_not_lower_the_bar() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), unclear()), -1);
    assert_eq!(walk.update(-1, stats(12_000_000, 156), kept_up()), 1);
    // 15 MB beats the slow window but trails the decayed 16.9 MB.
    assert_eq!(walk.update(1, stats(15_000_000, 170), kept_up()), -1);
}

fn behind() -> OutputWindow {
    window(0, 64, 0)
}

#[test]
fn a_descent_that_slows_the_sender_reverts() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), behind()), -2);
    // -2 compresses faster but carries 17% less: the output limits.
    assert_eq!(walk.update(-2, stats(15_000_000, 150), behind()), -1);
}

#[test]
fn a_descent_that_carries_more_holds_then_keeps_going() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.update(-1, stats(18_000_000, 156), behind()), -2);
    assert_eq!(walk.update(-2, stats(20_000_000, 150), behind()), -2);
    assert_eq!(walk.update(-2, stats(20_000_000, 150), behind()), -3);
}

#[test]
fn a_climb_that_carries_more_holds_despite_a_backlog() {
    let mut walk = LevelWalk::new(-7..=4, -2);
    assert_eq!(walk.update(-2, stats(19_000_000, 160), kept_up()), -1);
    // Batches overflow at -1, but it carries 20% more.
    assert_eq!(walk.update(-1, stats(22_800_000, 196), behind()), -1);
}
