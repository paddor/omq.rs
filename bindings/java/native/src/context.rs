//! Context handles and lifecycle.

use crate::error::{guard, jni_error, throw_java, throw_omq};
use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jboolean, jint, jlong, jlongArray};
use omq_tokio::{Context, ContextConfig, Error};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct JavaContext {
    ctx: Context,
    closed: AtomicBool,
}

impl JavaContext {
    pub(crate) const fn context(&self) -> &Context {
        &self.ctx
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

pub(crate) fn context_from_handle(handle: jlong) -> Result<&'static JavaContext, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    let ctx = unsafe { &*(handle as *mut JavaContext) };
    if ctx.closed.load(Ordering::Acquire) {
        return Err(Error::Closed);
    }
    Ok(ctx)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextCreate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    io_threads: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        if io_threads <= 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "ioThreads must be greater than zero",
            );
            return 0;
        }

        let ctx = Context::with_config(ContextConfig {
            io_threads: io_threads as usize,
        });
        Box::into_raw(Box::new(JavaContext {
            ctx,
            closed: AtomicBool::new(false),
        })) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextClose(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    owner: jboolean,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }

        let ctx = unsafe { Box::from_raw(handle as *mut JavaContext) };
        let was_open = !ctx.closed.swap(true, Ordering::AcqRel);
        if owner != 0 && was_open {
            ctx.ctx.term();
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextShareKey(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) -> jlongArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let ctx = context_from_handle(handle)?;
            let key = ctx.ctx.share_key();
            let high = (key >> 64) as u64 as jlong;
            let low = key as u64 as jlong;
            let out = env.new_long_array(2).map_err(jni_error)?;
            env.set_long_array_region(&out, 0, &[high, low])
                .map_err(jni_error)?;
            Ok(out.into_raw())
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_contextFromShareKey(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    high: jlong,
    low: jlong,
) -> jlong {
    guard(&mut env, 0, |_env| {
        let key = ((high as u64 as u128) << 64) | (low as u64 as u128);
        let Some(ctx) = Context::from_share_key(key) else {
            return 0;
        };
        Box::into_raw(Box::new(JavaContext {
            ctx,
            closed: AtomicBool::new(false),
        })) as jlong
    })
}
