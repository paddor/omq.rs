mod model;

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

fn pace(level: i32, rate: u64, window: u64) -> Pace {
    Pace {
        level,
        rate,
        window,
    }
}

#[test]
fn a_base_the_walk_does_not_return_to_gives_way() {
    let mut walk = LevelWalk::new(-7..=4, -1);
    assert_eq!(walk.compare(pace(1, 100, 1)), Verdict::Even);
    assert_eq!(
        walk.compare(pace(-1, 50, 2)),
        Verdict::Behind(Bottleneck::Output)
    );
    // 1 was not measured for `BASE_WINDOWS` windows: -1 sets the pace.
    assert_eq!(walk.compare(pace(-1, 50, 1 + BASE_WINDOWS)), Verdict::Even);
    assert_eq!(walk.compare(pace(-2, 60, 2 + BASE_WINDOWS)), Verdict::Ahead);
}

#[test]
fn the_safe_level_is_minus_four_or_the_nearest_in_range() {
    assert_eq!(LevelWalk::new(-7..=4, -1).safe, -4);
    assert_eq!(LevelWalk::new(1..=4, 1).safe, 1);
    assert_eq!(LevelWalk::new(-7..=-5, -5).safe, -5);
}

/// Holds `walk` at `level` with `plain` bytes per window until it checks
/// the safe level, and returns the windows that took.
fn hold_until_check(walk: &mut LevelWalk, level: i32, plain: u64) -> u64 {
    let start = walk.window;
    while walk.update(level, stats(plain, 200), unclear()) == level {}
    assert_eq!(walk.level(), -4);
    walk.window - start
}

#[test]
fn a_safe_level_that_carries_more_takes_over() {
    let mut walk = LevelWalk::new(-7..=4, 2);
    assert_eq!(hold_until_check(&mut walk, 2, 10_000_000), FIRST_CHECK);
    assert_eq!(walk.update(-4, stats(15_000_000, 180), unclear()), -4);
    // The walk starts over at -4.
    assert_eq!(walk.update(-4, stats(15_000_000, 180), unclear()), -4);
    assert_eq!(walk.update(-4, stats(15_000_000, 180), kept_up()), -3);
}

#[test]
fn checks_that_keep_the_walk_grow_apart() {
    let mut walk = LevelWalk::new(-7..=4, 2);
    let mut gaps = Vec::new();
    for _ in 0..5 {
        gaps.push(hold_until_check(&mut walk, 2, 10_000_000));
        assert_eq!(walk.update(-4, stats(9_000_000, 180), unclear()), 2);
    }
    assert_eq!(gaps, [16, 128, 256, 256, 256]);
}

#[test]
fn a_walk_that_settles_elsewhere_is_checked_soon() {
    let mut walk = LevelWalk::new(-7..=4, 2);
    hold_until_check(&mut walk, 2, 10_000_000);
    assert_eq!(walk.update(-4, stats(9_000_000, 180), unclear()), 2);
    // Within a quarter of the checked rate: the next check is 128 windows away.
    assert_eq!(walk.update(2, stats(8_000_000, 200), unclear()), 2);
    // Its best recent rate falls below 3/4 of the checked one in four windows.
    assert_eq!(hold_until_check(&mut walk, 2, 5_000_000), 4 + FIRST_CHECK);
}

#[test]
fn a_check_at_another_level_keeps_the_walk() {
    let mut walk = LevelWalk::new(-7..=4, 2);
    hold_until_check(&mut walk, 2, 10_000_000);
    // The encoder ran at 2 after all: no evidence for -4.
    assert_eq!(walk.update(2, stats(50_000_000, 200), unclear()), 2);
}

#[test]
fn huge_and_empty_windows_keep_the_level_in_range() {
    let mut rng = model::Rng::new(7);
    for range in [-7..=4, 1..=4, -7..=-5, -1..=-1] {
        let mut walk = LevelWalk::new(range.clone(), -1);
        for _ in 0..20_000 {
            let level = walk.level();
            let elapsed = rng.pick(&[0, 1, 100_000_000, 3_600_000_000_000]);
            let stats = CompressStats {
                plain: rng.pick(&[0, 1, 300_000, 1 << 40]),
                compressed: rng.pick(&[0, 1, 300_000, 1 << 41]),
                busy: Duration::from_nanos(rng.pick(&[0, 1, 50_000_000, elapsed])),
            };
            let output = OutputWindow {
                elapsed: Duration::from_nanos(elapsed),
                blocked: Duration::from_nanos(rng.pick(&[0, 1, elapsed / 8, elapsed, elapsed * 2])),
                batches: rng.pick(&[0, 1, 64, u32::MAX]),
                overflowed: rng.pick(&[0, 1, 64, u32::MAX]),
                input_full: rng.pick(&[0, 1, u32::MAX]),
            };
            walk.carry(rng.pick(&[0, 1, u64::MAX]));
            let next = walk.update(level, stats, output);
            assert!(range.contains(&next), "{range:?}: {next}");
        }
    }
}

#[test]
fn random_senders_keep_most_of_the_safe_level() {
    for seed in 0..400 {
        let mut rng = model::Rng::new(seed);
        let plant = model::random_plant(&mut rng);
        let mut walk = LevelWalk::new(-7..=4, -1);
        let levels = model::run(&mut walk, &plant, 300, &mut rng);
        let kept = model::against(&plant, &levels[30..], -4);
        assert!(
            kept >= 0.8,
            "seed {seed}: {kept:.3} of -4, {plant:?}\n{levels:?}"
        );
    }
}

#[test]
fn senders_that_change_keep_most_of_the_safe_level() {
    for seed in 0..300 {
        let mut rng = model::Rng::new(seed);
        let before = model::random_plant(&mut rng);
        let after = model::random_plant(&mut rng);
        let mut walk = LevelWalk::new(-7..=4, -1);
        model::run(&mut walk, &before, 200, &mut rng);
        let levels = model::run(&mut walk, &after, 300, &mut rng);
        let kept = model::against(&after, &levels[30..], -4);
        assert!(
            kept >= 0.8,
            "seed {seed}: {kept:.3} of -4, {after:?}\n{levels:?}"
        );
    }
}

#[test]
fn random_signals_keep_most_of_the_safe_level() {
    for range in [-7..=4, -1..=4, 1..=4, -7..=-1] {
        let safe = LevelWalk::new(range.clone(), -1).safe;
        for seed in 0..150 {
            let mut rng = model::Rng::new(seed);
            let plant = model::Plant {
                lies: 1.0,
                ..model::random_plant(&mut rng)
            };
            let mut walk = LevelWalk::new(range.clone(), -1);
            let levels = model::run(&mut walk, &plant, 300, &mut rng);
            assert!(levels.iter().all(|level| range.contains(level)));
            let kept = model::against(&plant, &levels[30..], safe);
            assert!(
                kept >= 0.8,
                "{range:?} seed {seed}: {kept:.3} of {safe}, {plant:?}"
            );
        }
    }
}

#[test]
fn prompts_over_a_saturated_link_climb_for_ratio() {
    for corpus in [model::PROSE, model::CODE, model::DOCS] {
        let plant = model::Plant {
            encoders: 4,
            noise: 0.05,
            ..model::Plant::new(
                corpus,
                Some(125e6),
                model::Load::Closed {
                    streams: 8,
                    think: 1e-3,
                },
            )
        };
        let mut rng = model::Rng::new(1);
        let mut walk = LevelWalk::new(-7..=4, -1);
        let levels = model::run(&mut walk, &plant, 400, &mut rng);
        let kept = model::efficiency(&plant, &levels[30..]);
        assert!(kept >= 0.95, "{kept:.3} of {:?}: {levels:?}", plant.best());
    }
}

#[test]
fn a_flood_without_a_link_limit_runs_fast() {
    for corpus in [model::PROSE, model::CODE, model::DOCS, model::SMALL] {
        let plant = model::Plant::new(corpus, None, model::Load::Open(None));
        let mut rng = model::Rng::new(1);
        let mut walk = LevelWalk::new(-7..=4, -1);
        let levels = model::run(&mut walk, &plant, 300, &mut rng);
        let kept = model::efficiency(&plant, &levels[30..]);
        assert!(kept >= 0.95, "{kept:.3} of {:?}: {levels:?}", plant.best());
    }
}

#[test]
fn a_single_stream_waiting_on_compression_falls_back() {
    // Nothing queues, so every window asks for ratio.
    let plant = model::Plant::new(
        model::CODE,
        None,
        model::Load::Closed {
            streams: 1,
            think: 50e-6,
        },
    );
    let mut rng = model::Rng::new(1);
    let mut walk = LevelWalk::new(-7..=4, -1);
    let levels = model::run(&mut walk, &plant, 300, &mut rng);
    let kept = model::against(&plant, &levels[30..], -4);
    assert!(kept >= 0.97, "{kept:.3} of -4: {levels:?}");
}
