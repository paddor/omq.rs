//! Blocking waits behind C API calls.
//!
//! Every wait races context shutdown, so `zmq_ctx_shutdown` and
//! `zmq_ctx_term` make blocked calls return `ETERM`. A call from an OMQ IO
//! thread, such as an async completion callback, fails with `EDEADLK`
//! instead of parking the thread its own work needs.

use std::ffi::c_int;
use std::future::Future;
use std::sync::Arc;

use crate::context::OmqContext;
use crate::error::ETERM;

/// Poll `future` on the calling thread until it completes or the context
/// shuts down.
pub(crate) fn block_on<T>(ctx: &OmqContext, future: impl Future<Output = T>) -> Result<T, c_int> {
    if omq_tokio::Context::on_io_thread() {
        return Err(libc::EDEADLK);
    }
    let handle = ctx.handle().ok_or(ETERM)?;
    handle.block_on(race_shutdown(ctx, future))
}

/// Run `future` on the context runtime and wait for its result.
pub(crate) fn run<T: Send + 'static>(
    ctx: &Arc<OmqContext>,
    future: impl Future<Output = T> + Send + 'static,
) -> Result<T, c_int> {
    if omq_tokio::Context::on_io_thread() {
        return Err(libc::EDEADLK);
    }
    let handle = ctx.handle().ok_or(ETERM)?;
    let (tx, rx) = flume::bounded(1);
    let ctx = ctx.clone();
    handle.spawn(async move {
        let _ = tx.send(race_shutdown(&ctx, future).await);
    });
    // A runtime torn down under the task drops the sender.
    rx.recv().unwrap_or(Err(ETERM))
}

async fn race_shutdown<T>(ctx: &OmqContext, future: impl Future<Output = T>) -> Result<T, c_int> {
    tokio::select! {
        biased;
        () = ctx.wait_for_shutdown() => Err(ETERM),
        output = future => Ok(output),
    }
}
