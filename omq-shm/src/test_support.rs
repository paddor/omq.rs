//! Helpers shared by the unit and Miri tests.

use omq_proto::{Message, PayloadPool};

/// A grouped message for routing id 7 whose body fills a pooled `size` slot
/// with `value`.
pub(crate) fn grouped(
    pool: &PayloadPool,
    prefix: &'static [u8],
    size: usize,
    value: u8,
) -> Message {
    let body = pool
        .try_message(size, |bytes| bytes.fill(value))
        .unwrap()
        .expect("free pool slot");

    Message::with_prefix(prefix.into(), body).with_routing_id(7)
}
