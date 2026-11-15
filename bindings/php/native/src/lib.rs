//! `zmq` PHP extension backed by `omq-libzmq`.
//!
//! Registers the module as `zmq` with ext-zmq 1.1.3's classes, so code and
//! Composer requirements written for ext-zmq run on OMQ unchanged.

mod context;
mod device;
mod errors;
mod fd_stream;
mod poll;
mod socket;
mod sockopt;
mod util;
mod zmq_class;

use ext_php_rs::prelude::*;

/// The ext-zmq API level this module implements, reported as its version.
const EXT_ZMQ_API_VERSION: &str = "1.1.3";

extern "C" fn module_shutdown(_type: i32, _module_number: i32) -> i32 {
    // Sockets first: terminating a context waits for its sockets.
    socket::shutdown();
    context::shutdown();
    0
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .name("zmq")
        .version(EXT_ZMQ_API_VERSION)
        .shutdown_function(module_shutdown)
        .class::<errors::ZmqException>()
        .class::<errors::ZmqContextException>()
        .class::<errors::ZmqSocketException>()
        .class::<errors::ZmqPollException>()
        .class::<errors::ZmqDeviceException>()
        .class::<zmq_class::Zmq>()
        .class::<context::ZmqContext>()
        .class::<socket::ZmqSocket>()
        .class::<poll::ZmqPoll>()
        .class::<device::ZmqDevice>()
}
