//! Native ring message encoding.

use omq_tokio::Message;

pub(crate) fn encoded_message_len(message: &Message) -> usize {
    if message.len() == 1 {
        return message.part_slice(0).unwrap_or_default().len();
    }
    4 + 4 * message.len() + message.byte_len()
}

pub(crate) fn encode_message(message: &Message) -> Vec<u8> {
    let mut out = vec![0; encoded_message_len(message)];
    write_message_encoded(message, &mut out);
    out
}

pub(crate) fn write_message_encoded(message: &Message, out: &mut [u8]) {
    if message.len() == 1 {
        let body = message.part_slice(0).unwrap_or_default();
        out.copy_from_slice(body);
        return;
    }

    let part_count = message.len() as u32;
    out[..4].copy_from_slice(&part_count.to_ne_bytes());
    let mut offset = 4 + 4 * message.len();
    for index in 0..message.len() {
        let part = message.part_slice(index).unwrap_or_default();
        let len_offset = 4 + 4 * index;
        out[len_offset..len_offset + 4].copy_from_slice(&(part.len() as u32).to_ne_bytes());
        out[offset..offset + part.len()].copy_from_slice(part);
        offset += part.len();
    }
}

#[cfg(test)]
mod tests;
