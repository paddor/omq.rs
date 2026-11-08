//! Message conversion and foreign buffer ownership.

use crate::convert::byte_array;
use crate::error::jni_error;
use crate::identity::identity_object;
use bytes::Bytes;
use jni::JNIEnv;
use jni::objects::{JByteArray, JObject, JObjectArray, JValue};
use jni::sys::jint;
use omq_tokio::{Error, Message};

pub(crate) fn received_message_object<'local>(
    env: &mut JNIEnv<'local>,
    mut message: Message,
) -> Result<JObject<'local>, Error> {
    let identity = message
        .pop_identity()
        .ok_or_else(|| Error::Config("message has no sender snapshot".into()))?;
    let identity = identity_object(env, &identity)?;
    let message = message_to_java_object(env, message)?;
    env.new_object(
        "io/omq/ReceivedMessage",
        "(Lio/omq/Identity;Lio/omq/Message;)V",
        &[JValue::Object(&identity), JValue::Object(&message)],
    )
    .map_err(jni_error)
}

pub(crate) fn bytes_from_parts(
    env: &mut JNIEnv<'_>,
    parts: JObjectArray<'_>,
) -> Result<Vec<Bytes>, Error> {
    let len = env.get_array_length(&parts).map_err(jni_error)?;
    let mut out = Vec::with_capacity(len as usize);
    for i in 0..len {
        let part = env.get_object_array_element(&parts, i).map_err(jni_error)?;
        if part.is_null() {
            return Err(Error::Config("message part must not be null".to_string()));
        }
        let part = JByteArray::from(part);
        out.push(Bytes::from(byte_array(env, part)?));
    }
    Ok(out)
}

pub(crate) fn java_message(parts: Vec<Bytes>, routing_id: jint) -> Result<Message, Error> {
    if routing_id < 0 {
        return Err(Error::Config("routing ID must be non-negative".to_string()));
    }
    let message = Message::multipart(parts);
    Ok(if routing_id == 0 {
        message
    } else {
        message.with_routing_id(routing_id as u32)
    })
}

fn message_to_java_parts<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObjectArray<'local>, Error> {
    let byte_array_class = env.find_class("[B").map_err(jni_error)?;
    let parts = env
        .new_object_array(message.len() as jint, byte_array_class, JObject::null())
        .map_err(jni_error)?;

    for i in 0..message.len() {
        let array = message_part_to_java(env, message, i)?;
        env.set_object_array_element(&parts, i as jint, array)
            .map_err(jni_error)?;
    }

    Ok(parts)
}

fn message_part_to_java<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
    index: usize,
) -> Result<JByteArray<'local>, Error> {
    env.byte_array_from_slice(message.part_slice(index).unwrap_or_default())
        .map_err(jni_error)
}

pub(crate) fn message_to_java_object<'local>(
    env: &mut JNIEnv<'local>,
    message: Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_object_ref(env, &message)
}

fn message_to_java_object_ref<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObject<'local>, Error> {
    let routing_id = message.routing_id().unwrap_or(0);
    if message.len() == 1 {
        let part = message_part_to_java(env, message, 0)?;
        let part = JObject::from(part);
        return env
            .call_static_method(
                "io/omq/Message",
                "fromNative",
                "([BI)Lio/omq/Message;",
                &[JValue::Object(&part), JValue::Int(routing_id as jint)],
            )
            .and_then(|value| value.l())
            .map_err(jni_error);
    }

    let parts = message_to_java_parts(env, message)?;
    let parts = JObject::from(parts);
    env.call_static_method(
        "io/omq/Message",
        "fromNative",
        "([[BI)Lio/omq/Message;",
        &[JValue::Object(&parts), JValue::Int(routing_id as jint)],
    )
    .and_then(|value| value.l())
    .map_err(jni_error)
}

pub(crate) fn message_to_java_native<'local>(
    env: &mut JNIEnv<'local>,
    message: Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_native_ref(env, &message)
}

fn message_to_java_native_ref<'local>(
    env: &mut JNIEnv<'local>,
    message: &Message,
) -> Result<JObject<'local>, Error> {
    message_to_java_object_ref(env, message)
}
