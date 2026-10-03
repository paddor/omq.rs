//! Direct external receive queue and bounded fallback admission.

use std::sync::Arc;

use omq_tokio::Message;
use omq_tokio::engine::RecvSinkConfig;

pub(crate) struct RecvConsumers {
    fast: yring::Consumer<Message>,
    pump: yring::Consumer<Message>,
    prefer_pump: bool,
}

impl RecvConsumers {
    pub(crate) fn new(fast: yring::Consumer<Message>, pump: yring::Consumer<Message>) -> Self {
        Self {
            fast,
            pump,
            prefer_pump: false,
        }
    }

    pub(crate) fn refresh(&mut self, config: Option<&Arc<RecvSinkConfig>>) {
        if self.fast.is_disconnected()
            && let Some(consumer) = config.and_then(|config| config.try_take_pending_consumer())
        {
            self.fast = consumer;
        }
    }

    #[expect(
        clippy::len_zero,
        reason = "length only chooses the source; failed pops register waiters"
    )]
    pub(crate) fn try_pop(&mut self) -> Option<(Message, bool)> {
        if self.prefer_pump && self.pump.len() > 0 {
            self.prefer_pump = false;
            return self.pump.prefetch_and_pop_with_full();
        }
        if let Some(message) = self.fast.prefetch_and_pop_with_full() {
            self.prefer_pump = true;
            return Some(message);
        }
        self.prefer_pump = false;
        self.pump.prefetch_and_pop_with_full()
    }

    pub(crate) fn has_data(&self) -> bool {
        // Check both: each empty check registers its own producer wake.
        let fast_empty = self.fast.is_empty();
        let pump_empty = self.pump.is_empty();
        !fast_empty || !pump_empty
    }
}
