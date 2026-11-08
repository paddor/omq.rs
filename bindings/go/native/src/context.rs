//! Context handles and lifecycle.

use crate::error::{CLOSED, CONFIG};
use crate::ffi::OmqGoStatus;
use crate::stats::{CONTEXTS_CREATED, CONTEXTS_FREED, bump};
use omq_tokio::{Context, ContextConfig};
use std::sync::atomic::{AtomicBool, Ordering};

pub struct OmqGoContext {
    ctx: Context,
    owner: bool,
    closed: AtomicBool,
}

impl OmqGoContext {
    pub(crate) const fn context(&self) -> &Context {
        &self.ctx
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_open(
    io_threads: usize,
    out: *mut *mut OmqGoContext,
) -> OmqGoStatus {
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null context output pointer");
    }
    if io_threads == 0 {
        return OmqGoStatus::err(CONFIG, "io_threads must be greater than zero");
    }

    let ctx = Context::with_config(ContextConfig { io_threads });
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoContext {
            ctx,
            owner: true,
            closed: AtomicBool::new(false),
        }));
    }
    bump(&CONTEXTS_CREATED);
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_from_share_key(
    high: u64,
    low: u64,
    out: *mut *mut OmqGoContext,
) -> OmqGoStatus {
    if out.is_null() {
        return OmqGoStatus::err(CONFIG, "null context output pointer");
    }
    let key = ((high as u128) << 64) | low as u128;
    let Some(ctx) = Context::from_share_key(key) else {
        return OmqGoStatus::err(CLOSED, "shared context not found");
    };
    unsafe {
        *out = Box::into_raw(Box::new(OmqGoContext {
            ctx,
            owner: false,
            closed: AtomicBool::new(false),
        }));
    }
    bump(&CONTEXTS_CREATED);
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_share_key(
    ctx: *mut OmqGoContext,
    high: *mut u64,
    low: *mut u64,
) -> OmqGoStatus {
    if ctx.is_null() || high.is_null() || low.is_null() {
        return OmqGoStatus::err(CONFIG, "null context share key pointer");
    }
    let ctx = unsafe { &*ctx };
    if ctx.closed.load(Ordering::Acquire) {
        return OmqGoStatus::err(CLOSED, "context closed");
    }
    let key = ctx.ctx.share_key();
    unsafe {
        *high = (key >> 64) as u64;
        *low = key as u64;
    }
    OmqGoStatus::ok()
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_close(ctx: *mut OmqGoContext) {
    if ctx.is_null() {
        return;
    }
    let ctx = unsafe { &*ctx };
    let was_open = !ctx.closed.swap(true, Ordering::AcqRel);
    if was_open && ctx.owner {
        ctx.ctx.term();
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn omq_go_context_free(ctx: *mut OmqGoContext) {
    if ctx.is_null() {
        return;
    }
    omq_go_context_close(ctx);
    bump(&CONTEXTS_FREED);
    unsafe {
        drop(Box::from_raw(ctx));
    }
}
