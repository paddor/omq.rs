//! Per-pipe options for `Socket::connect_with`.
//!
//! Gated on the `priority` feature. Backends that don't enable the
//! feature don't see this type and pay no overhead - round-robin send
//! stays on the shared queue / per-socket HWM model.

use std::num::NonZeroU8;

/// Default priority - middle of the `u8` range. Leaves equal headroom
/// above and below for users who want to assign higher / lower
/// priority later.
pub const DEFAULT_PRIORITY: u8 = 128;

/// Per-endpoint options carried by [`Socket::connect_with`]. Currently
/// just `priority`; designed to grow additively (each new field has a
/// sensible default).
///
/// Priority semantics are **strict**, not weighted: a send always goes
/// to the highest-priority pipe that's ready (lower number = higher
/// priority). Lower-priority pipes only receive traffic when higher
/// ones are blocked or disconnected. Round-robin within a priority
/// level. Lower-priority pipes can starve indefinitely under a hot
/// high-priority pipe - by design.
///
/// [`Socket::connect_with`]: in the backend crates (omq-compio /
/// omq-tokio).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnectOpts {
    /// Pipe priority. 1 (highest) .. 255 (lowest). Default
    /// [`DEFAULT_PRIORITY`] (128).
    pub priority: NonZeroU8,
}

impl ConnectOpts {
    /// Construct with the default priority.
    pub const fn new() -> Self {
        // SAFETY: DEFAULT_PRIORITY is a non-zero const.
        Self {
            priority: match NonZeroU8::new(DEFAULT_PRIORITY) {
                Some(p) => p,
                None => unreachable!(),
            },
        }
    }

    /// Set the priority for this endpoint. Lower number = higher
    /// priority. Pass any `NonZeroU8`; values are not clamped.
    #[must_use]
    pub const fn priority(mut self, p: NonZeroU8) -> Self {
        self.priority = p;
        self
    }
}

impl Default for ConnectOpts {
    fn default() -> Self {
        Self::new()
    }
}
