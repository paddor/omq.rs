use super::*;
use omq_proto::proto::SocketType;
use omq_proto::proto::connection::{ConnectionConfig, Role};
use omq_proto::{CompressionKind, Options};
use std::fmt::Write as _;

fn state(options: &Options) -> OutboundState {
    let (encoder, _) = MessageEncoder::for_compression_kind(CompressionKind::Zstd, options)
        .unwrap()
        .unwrap();

    OutboundState::new(Some(encoder), None, 0, None)
}

fn level(state: &OutboundState) -> i32 {
    state
        .encoder
        .as_ref()
        .and_then(MessageEncoder::level)
        .unwrap()
}

/// Encode one full window of backlogged input and backdate its start.
fn fill_window(state: &mut OutboundState, limited: bool, input_full: bool) {
    let mut connection = Connection::new(ConnectionConfig::new(Role::Client, SocketType::Push));
    let mut eq = FrameBuffer::new();
    let mut body = String::new();

    for index in 0..8192 {
        write!(body, "{{\"id\":{}}}", index % 89).unwrap();
    }

    let message = Message::single(body);
    while !state.level_window_due() {
        state
            .batch_encode(&message, || None, &mut connection, &mut eq)
            .unwrap();
        state.note_batch(true, limited, input_full);

        if let Some(window) = &mut state.level_window {
            window.started = Instant::now().checked_sub(ADAPT_WINDOW).unwrap();
        }
    }
}

#[test]
fn stalled_writes_raise_and_overflowing_input_lowers_the_level() {
    let mut stalled = state(&Options::default());
    assert_eq!(level(&stalled), -1);
    fill_window(&mut stalled, true, true);
    stalled.adapt_level(Instant::now(), ADAPT_WINDOW);
    assert_eq!(level(&stalled), 1);
    assert!(!stalled.level_window_due());

    let mut queued = state(&Options::default());
    fill_window(&mut queued, false, false);
    queued.adapt_level(Instant::now(), Duration::ZERO);
    assert_eq!(level(&queued), -1);

    for (limited, input_full) in [(true, false), (false, true)] {
        let mut behind = state(&Options::default());
        fill_window(&mut behind, limited, input_full);
        behind.adapt_level(Instant::now(), Duration::ZERO);
        assert_eq!(level(&behind), -2);
    }
}

#[test]
fn fixed_levels_do_not_track_windows() {
    let mut fixed = state(&Options::default().compression_level(3));
    assert!(!fixed.adapts_level());
    fixed.note_batch(true, true, true);
    assert!(!fixed.level_window_due());
}
