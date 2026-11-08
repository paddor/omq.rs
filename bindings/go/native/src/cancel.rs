//! Native cancellation handles.

use crate::stats::{CANCELS_CREATED, CANCELS_FREED, bump};
use omq_tokio::blocking::BlockingRecvCancel;

pub struct OmqGoCancel {
    inner: BlockingRecvCancel,
}

impl OmqGoCancel {
    pub(crate) const fn token(&self) -> &BlockingRecvCancel {
        &self.inner
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_new() -> *mut OmqGoCancel {
    let cancel = Box::into_raw(Box::new(OmqGoCancel {
        inner: BlockingRecvCancel::new(),
    }));
    bump(&CANCELS_CREATED);
    cancel
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel(cancel: *const OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    unsafe {
        (*cancel).inner.cancel();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_register_current(cancel: *const OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    unsafe {
        (*cancel).inner.register_current_thread_once();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_cancel_free(cancel: *mut OmqGoCancel) {
    if cancel.is_null() {
        return;
    }
    bump(&CANCELS_FREED);
    unsafe {
        drop(Box::from_raw(cancel));
    }
}
