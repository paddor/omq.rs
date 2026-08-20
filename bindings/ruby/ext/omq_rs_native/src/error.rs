use omq_proto::error::Error as OmqError;

use crate::rb::RubyErr;

pub fn map_err(e: OmqError) -> RubyErr {
    match e {
        OmqError::Closed => RubyErr::io("socket closed"),
        OmqError::Timeout => RubyErr::runtime("operation timed out"),
        OmqError::Unroutable => RubyErr::runtime("no route to peer"),
        OmqError::InvalidEndpoint(msg) => RubyErr::arg(msg),
        OmqError::Protocol(msg) | OmqError::HandshakeFailed(msg) => RubyErr::runtime(msg),
        OmqError::Io(e) => RubyErr::runtime(e.to_string()),
        _ => RubyErr::runtime(format!("{e}")),
    }
}
