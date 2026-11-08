//! Receive operations and buffer conversion.

use crate::error::{guard, jni_error, throw_java, throw_java_default, throw_omq};
use crate::message::{message_to_java_native, received_message_object};
use crate::socket::socket_from_handle;
use jni::JNIEnv;
use jni::objects::{GlobalRef, JByteArray, JByteBuffer, JClass, JObject, JValue};
use jni::sys::{jint, jlong, jobject, jsize};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message};
use std::time::Duration;

fn recv_with_timeout(socket: &BlockingSocket, timeout_millis: jlong) -> Result<Message, Error> {
    if timeout_millis < 0 {
        socket.recv()
    } else if timeout_millis == 0 {
        socket.try_recv()
    } else {
        socket.recv_timeout(Duration::from_millis(timeout_millis as u64))
    }
}

fn byte_buffer_int(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    method: &str,
) -> Result<jint, Error> {
    env.call_method(buffer, method, "()I", &[])
        .and_then(|value| value.i())
        .map_err(jni_error)
}

fn byte_buffer_bool(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    method: &str,
) -> Result<bool, Error> {
    env.call_method(buffer, method, "()Z", &[])
        .and_then(|value| value.z())
        .map_err(jni_error)
}

fn set_byte_buffer_position(
    env: &mut JNIEnv<'_>,
    buffer: &JObject<'_>,
    position: jint,
) -> Result<(), Error> {
    env.call_method(
        buffer,
        "position",
        "(I)Ljava/nio/Buffer;",
        &[JValue::Int(position)],
    )
    .map(|_| ())
    .map_err(jni_error)
}

fn jbyte_slice(bytes: &[u8]) -> &[i8] {
    // Java byte is signed; JNI copies raw byte values without conversion.
    unsafe { std::slice::from_raw_parts(bytes.as_ptr().cast::<i8>(), bytes.len()) }
}

fn write_message_to_byte_buffer(
    env: &mut JNIEnv<'_>,
    buffer: JObject<'_>,
    message: &Message,
) -> Result<usize, Error> {
    if message.len() != 1 {
        throw_java(
            env,
            "java/lang/IllegalStateException",
            format!("message has {} parts", message.len()),
        );
        return Err(Error::Config("message is multipart".to_string()));
    }
    if byte_buffer_bool(env, &buffer, "isReadOnly")? {
        throw_java_default(env, "java/nio/ReadOnlyBufferException");
        return Err(Error::Config("destination is read-only".to_string()));
    }

    let body = message.part_slice(0).unwrap_or_default();
    let remaining = byte_buffer_int(env, &buffer, "remaining")?;
    if body.len() > remaining as usize {
        throw_java_default(env, "java/nio/BufferOverflowException");
        return Err(Error::Config(
            "destination has insufficient remaining space".to_string(),
        ));
    }

    let position = byte_buffer_int(env, &buffer, "position")?;
    if byte_buffer_bool(env, &buffer, "isDirect")? {
        let direct = <&JByteBuffer>::from(&buffer);
        let capacity = env.get_direct_buffer_capacity(direct).map_err(jni_error)?;
        let end = position as usize + body.len();
        if end > capacity {
            throw_java_default(env, "java/nio/BufferOverflowException");
            return Err(Error::Config(
                "destination has insufficient direct capacity".to_string(),
            ));
        }
        let base = env.get_direct_buffer_address(direct).map_err(jni_error)?;
        unsafe {
            std::ptr::copy_nonoverlapping(body.as_ptr(), base.add(position as usize), body.len());
        }
    } else if byte_buffer_bool(env, &buffer, "hasArray")? {
        let array_offset = byte_buffer_int(env, &buffer, "arrayOffset")?;
        let array = env
            .call_method(&buffer, "array", "()[B", &[])
            .and_then(|value| value.l())
            .map(JByteArray::from)
            .map_err(jni_error)?;
        let start = array_offset
            .checked_add(position)
            .ok_or_else(|| Error::Config("byte buffer offset overflow".to_string()))?;
        env.set_byte_array_region(&array, start as jsize, jbyte_slice(body))
            .map_err(jni_error)?;
    } else {
        throw_java(
            env,
            "java/lang/UnsupportedOperationException",
            "ByteBuffer must be direct or array-backed",
        );
        return Err(Error::Config(
            "destination is neither direct nor array-backed".to_string(),
        ));
    }

    set_byte_buffer_position(env, &buffer, position + body.len() as jint)?;
    Ok(body.len())
}

pub(crate) fn recv_many_into(
    socket: &BlockingSocket,
    max_messages: jint,
    timeout_millis: jlong,
    out: &mut Vec<Message>,
) -> Result<usize, Error> {
    if max_messages <= 0 {
        return Err(Error::Config(
            "maxMessages must be greater than zero".to_string(),
        ));
    }

    let max = max_messages as usize;
    if timeout_millis < 0 {
        socket.recv_many_into(max, out)
    } else if timeout_millis == 0 {
        socket.try_recv_many_into(max, out)
    } else {
        socket.recv_many_timeout_into(max, Duration::from_millis(timeout_millis as u64), out)
    }
}

fn try_receive_any(
    entries: &[(BlockingSocket, GlobalRef)],
) -> Result<Option<(GlobalRef, Message)>, Error> {
    for (socket, java_socket) in entries {
        match socket.try_recv() {
            Ok(message) => return Ok(Some((java_socket.clone(), message))),
            Err(Error::WouldBlock | Error::Timeout) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

pub(crate) async fn receive_any_loop(
    entries: Vec<(BlockingSocket, GlobalRef)>,
    timeout: Option<Duration>,
) -> Result<Option<(GlobalRef, Message)>, Error> {
    if let Some(event) = try_receive_any(&entries)? {
        return Ok(Some(event));
    }
    if timeout.is_some_and(|timeout| timeout.is_zero()) {
        return Ok(None);
    }
    // Async receives are cancel-safe: the losers consume nothing.
    let receives = entries.iter().map(|(socket, java_socket)| {
        let socket = socket.clone_shared().into_async();
        Box::pin(async move {
            socket
                .recv()
                .await
                .map(|message| (java_socket.clone(), message))
        })
    });
    let any = futures::future::select_all(receives);
    match timeout {
        None => any.await.0.map(Some),
        Some(timeout) => match tokio::time::timeout(timeout, any).await {
            Ok((result, _, _)) => result.map(Some),
            Err(_) => Ok(None),
        },
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecv(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = recv_with_timeout(&socket, timeout_millis)?;
            message_to_java_native(env, message).map(JObject::into_raw)
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
pub extern "system" fn Java_io_omq_Native_socketRecvFrom(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = recv_with_timeout(&socket, timeout_millis)?;
            received_message_object(env, message).map(JObject::into_raw)
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
pub extern "system" fn Java_io_omq_Native_socketRecvInto(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    destination: JObject<'_>,
    timeout_millis: jlong,
) -> jint {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?.materialize()?;
            let message = recv_with_timeout(&socket, timeout_millis)?;
            write_message_to_byte_buffer(env, destination, &message).map(|len| len as jint)
        })();

        match result {
            Ok(len) => len,
            Err(error) => {
                if env.exception_check().unwrap_or(false) {
                    return 0;
                }
                throw_omq(env, error);
                0
            }
        }
    })
}
