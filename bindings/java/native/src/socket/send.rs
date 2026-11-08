//! Send operations.

use crate::convert::{byte_array, duration_from_millis};
use crate::error::{guard, throw_omq};
use crate::message::{bytes_from_parts, java_message};
use crate::socket::socket_from_handle;
use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JByteArray, JClass, JObjectArray};
use jni::sys::{jint, jlong};
use omq_proto::TrySendError;
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message, SocketType};
use std::collections::VecDeque;

fn java_try_send(
    socket: &BlockingSocket,
    message: Message,
) -> core::result::Result<(), TrySendError> {
    match socket.socket_type() {
        SocketType::Scatter if message.len() != 1 => {
            Err(TrySendError::Error(Error::Protocol(format!(
                "Scatter socket requires single-part messages (got {})",
                message.len()
            ))))
        }
        SocketType::Push | SocketType::Scatter => {
            let mut messages = VecDeque::with_capacity(1);
            messages.push_back(message);
            match socket.try_send_many(&mut messages, 1) {
                Ok(sent) if sent > 0 => Ok(()),
                Ok(_) => {
                    let message = messages
                        .pop_front()
                        .expect("try_send_many returned zero with message pending");
                    Err(TrySendError::Full(message))
                }
                Err(error) => Err(error),
            }
        }
        _ => socket.try_send(message),
    }
}

fn java_send(socket: &BlockingSocket, message: Message) -> Result<(), Error> {
    match java_try_send(socket, message) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(returned)) => socket.send(returned),
        Err(TrySendError::Closed) => Err(Error::Closed),
        Err(TrySendError::Error(error)) => Err(error),
    }
}

fn send_with_timeout(
    socket: &BlockingSocket,
    message: Message,
    timeout_millis: jlong,
) -> Result<bool, Error> {
    if timeout_millis < 0 {
        socket.send(message)?;
        return Ok(true);
    }

    let timeout = duration_from_millis(timeout_millis)?;
    match java_try_send(socket, message) {
        Ok(()) => Ok(true),
        Err(TrySendError::Full(returned)) => match socket.send_timeout(returned, timeout) {
            Ok(()) => Ok(true),
            Err(Error::Timeout) => Ok(false),
            Err(error) => Err(error),
        },
        Err(TrySendError::Closed) => Err(Error::Closed),
        Err(TrySendError::Error(error)) => Err(error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSend(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    data: JByteArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let data = byte_array(env, data)?;
            java_send(&socket.materialize()?, Message::single(Bytes::from(data)))
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendMultipart(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            java_send(&socket.materialize()?, java_message(parts, routing_id)?)
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendMultipartTimeout(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            send_with_timeout(
                &socket.materialize()?,
                java_message(parts, routing_id)?,
                timeout_millis,
            )
            .map(i32::from)
        })();

        match result {
            Ok(sent) => sent,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketTrySendMultipart(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let parts = bytes_from_parts(env, parts)?;
            match java_try_send(&socket.materialize()?, java_message(parts, routing_id)?) {
                Ok(()) => Ok(1),
                Err(TrySendError::Full(_)) => Ok(0),
                Err(TrySendError::Closed) => Err(Error::Closed),
                Err(TrySendError::Error(error)) => Err(error),
            }
        })();

        match result {
            Ok(value) => value,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}
