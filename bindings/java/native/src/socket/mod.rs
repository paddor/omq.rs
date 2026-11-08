//! Socket handles, materialization, and endpoint operations.

use crate::context::context_from_handle;
use crate::convert::{byte_array, duration_from_millis, java_string, socket_type_from_code};
use crate::error::{guard, jni_error, throw_java, throw_omq, throw_omq_for_endpoint};
use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jint, jlong, jstring};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Context, Endpoint, Error, Options, SocketType};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

pub(crate) struct JavaSocket {
    ctx: Context,
    socket_type: SocketType,
    options: Mutex<Options>,
    socket: OnceLock<BlockingSocket>,
    materialize_lock: Mutex<()>,
    closed: AtomicBool,
}

impl JavaSocket {
    pub(crate) const fn context(&self) -> &Context {
        &self.ctx
    }

    pub(crate) fn materialize(&self) -> Result<BlockingSocket, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone_shared());
        }

        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| Error::Config("materialize lock poisoned".to_string()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        if let Some(socket) = self.socket.get() {
            return Ok(socket.clone_shared());
        }

        let options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?
            .clone();
        options.validate()?;

        let created = self.ctx.blocking_socket(self.socket_type, options);
        self.socket
            .set(created.clone_shared())
            .map_err(|_| Error::Config("socket materialized concurrently".to_string()))?;
        Ok(created)
    }

    fn shutdown(&self) {
        let _guard = self
            .materialize_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(socket) = self.socket.get() {
            let _ = socket.clone().close();
        }
    }

    pub(crate) fn set_option<F>(&self, f: F) -> Result<(), Error>
    where
        F: FnOnce(&mut Options),
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        let _guard = self
            .materialize_lock
            .lock()
            .map_err(|_| Error::Config("materialize lock poisoned".to_string()))?;
        if self.socket.get().is_some() {
            return Err(Error::Config(
                "socket options must be set before bind/connect/send/receive".to_string(),
            ));
        }

        let mut options = self
            .options
            .lock()
            .map_err(|_| Error::Config("options lock poisoned".to_string()))?;
        let mut next = options.clone();
        f(&mut next);
        next.validate()?;
        *options = next;
        Ok(())
    }
}

pub(crate) fn socket_from_handle(handle: jlong) -> Result<&'static JavaSocket, Error> {
    if handle == 0 {
        return Err(Error::Closed);
    }
    Ok(unsafe { &*(handle as *mut JavaSocket) })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketCreate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    context_handle: jlong,
    socket_type: jint,
) -> jlong {
    guard(&mut env, 0, |env| {
        let ctx = match context_from_handle(context_handle) {
            Ok(ctx) => ctx,
            Err(error) => {
                throw_omq(env, error);
                return 0;
            }
        };
        if ctx.is_closed() {
            throw_omq(env, Error::Closed);
            return 0;
        }

        let socket_type = match socket_type_from_code(socket_type) {
            Ok(socket_type) => socket_type,
            Err(error) => {
                throw_omq(env, error);
                return 0;
            }
        };

        Box::into_raw(Box::new(JavaSocket {
            ctx: ctx.context().clone(),
            socket_type,
            options: Mutex::new(Options::default()),
            socket: OnceLock::new(),
            materialize_lock: Mutex::new(()),
            closed: AtomicBool::new(false),
        })) as jlong
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketShutdown(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |env| match socket_from_handle(handle) {
        Ok(socket) => socket.shutdown(),
        Err(error) => throw_omq(env, error),
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketClose(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }

        let socket = unsafe { Box::from_raw(handle as *mut JavaSocket) };
        socket.shutdown();
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketBind(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            let bound = socket.materialize()?.bind(endpoint)?;
            env.new_string(bound.to_string())
                .map(|s| s.into_raw())
                .map_err(jni_error)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq_for_endpoint(env, error, "bind", &endpoint_text);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketConnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.connect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "connect", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketUnbind(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.unbind(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "unbind", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketDisconnect(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    endpoint: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let mut endpoint_text = String::new();
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            endpoint_text = java_string(env, endpoint)?;
            let endpoint = Endpoint::from_str(&endpoint_text)?;
            socket.materialize()?.disconnect(endpoint)
        })();

        if let Err(error) = result {
            throw_omq_for_endpoint(env, error, "disconnect", &endpoint_text);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSubscribe(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    prefix: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let prefix = byte_array(env, prefix)?;
            socket.materialize()?.subscribe(Bytes::from(prefix))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketUnsubscribe(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    prefix: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let prefix = byte_array(env, prefix)?;
            socket.materialize()?.unsubscribe(Bytes::from(prefix))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketJoin(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    group: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let group = byte_array(env, group)?;
            socket.materialize()?.join(Bytes::from(group))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketLeave(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    group: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let group = byte_array(env, group)?;
            socket.materialize()?.leave(Bytes::from(group))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketWaitConnected(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    min_peers: jint,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, -1, |env| {
        if min_peers < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "minPeers must be non-negative",
            );
            return -1;
        }

        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = duration_from_millis(timeout_millis)?;
            let count = socket
                .materialize()?
                .wait_connected(min_peers as usize, timeout)?;
            Ok(count as jint)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                -1
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketWaitSubscribed(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    min_subscriptions: jlong,
    timeout_millis: jlong,
) -> jlong {
    guard(&mut env, -1, |env| {
        if min_subscriptions < 0 {
            throw_java(
                env,
                "java/lang/IllegalArgumentException",
                "minSubscriptions must be non-negative",
            );
            return -1;
        }

        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = duration_from_millis(timeout_millis)?;
            let count = socket
                .materialize()?
                .wait_subscribed(min_subscriptions as u64, timeout)?;
            Ok(count as jlong)
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                -1
            }
        }
    })
}

pub(crate) mod options;
pub(crate) mod recv;
pub(crate) mod send;
