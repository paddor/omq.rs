//! Foreign values and scalar option conversion.

use crate::error::jni_error;
use jni::JNIEnv;
use jni::objects::{JByteArray, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use omq_tokio::{Error, SocketType};
use std::time::Duration;

pub(crate) fn optional_java_string<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&str>,
) -> Result<JObject<'local>, Error> {
    value
        .map(|value| env.new_string(value).map(JObject::from).map_err(jni_error))
        .unwrap_or_else(|| Ok(JObject::null()))
}

pub(crate) fn optional_java_bytes<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&[u8]>,
) -> Result<JObject<'local>, Error> {
    value
        .map(|value| {
            env.byte_array_from_slice(value)
                .map(JObject::from)
                .map_err(jni_error)
        })
        .unwrap_or_else(|| Ok(JObject::null()))
}

pub(crate) fn nullable_string<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&str>,
) -> Result<JObject<'local>, Error> {
    match value {
        Some(value) => env.new_string(value).map(JObject::from).map_err(jni_error),
        None => Ok(JObject::null()),
    }
}

pub(crate) fn nullable_bytes<'local>(
    env: &mut JNIEnv<'local>,
    value: Option<&[u8]>,
) -> Result<JObject<'local>, Error> {
    match value {
        Some(value) => env
            .byte_array_from_slice(value)
            .map(JObject::from)
            .map_err(jni_error),
        None => Ok(JObject::null()),
    }
}

pub(crate) fn java_string(env: &mut JNIEnv<'_>, value: JString<'_>) -> Result<String, Error> {
    env.get_string(&value).map(|s| s.into()).map_err(jni_error)
}

pub(crate) fn java_strings(
    env: &mut JNIEnv<'_>,
    values: &JObjectArray<'_>,
) -> Result<Vec<String>, Error> {
    let len = env.get_array_length(values).map_err(jni_error)?;
    let mut out = Vec::with_capacity(len as usize);
    for index in 0..len {
        let value = env
            .get_object_array_element(values, index)
            .map_err(jni_error)?;
        if value.is_null() {
            return Err(Error::Config("PLAIN credential must not be null".into()));
        }
        out.push(java_string(env, JString::from(value))?);
    }
    Ok(out)
}

pub(crate) fn byte_array(env: &mut JNIEnv<'_>, value: JByteArray<'_>) -> Result<Vec<u8>, Error> {
    env.convert_byte_array(value).map_err(jni_error)
}

pub(crate) fn duration_from_millis(millis: jlong) -> Result<Duration, Error> {
    if millis < 0 {
        return Err(Error::Config("duration must be non-negative".to_string()));
    }
    Ok(Duration::from_millis(millis as u64))
}

pub(crate) fn optional_duration_from_millis(millis: jlong) -> Result<Option<Duration>, Error> {
    if millis == -1 {
        return Ok(None);
    }
    duration_from_millis(millis).map(Some)
}

pub(crate) fn optional_usize_from_long(name: &str, value: jlong) -> Result<Option<usize>, Error> {
    if value == -1 {
        return Ok(None);
    }
    if value < 0 {
        return Err(Error::Config(format!("{name} must be non-negative")));
    }
    Ok(Some(value as usize))
}

pub(crate) fn socket_type_from_code(code: jint) -> Result<SocketType, Error> {
    Ok(match code {
        0 => SocketType::Req,
        1 => SocketType::Rep,
        2 => SocketType::Pub,
        3 => SocketType::Sub,
        4 => SocketType::XPub,
        5 => SocketType::XSub,
        6 => SocketType::Push,
        7 => SocketType::Pull,
        8 => SocketType::Dealer,
        9 => SocketType::Router,
        10 => SocketType::Pair,
        11 => SocketType::Client,
        12 => SocketType::Server,
        13 => SocketType::Radio,
        14 => SocketType::Dish,
        15 => SocketType::Scatter,
        16 => SocketType::Gather,
        17 => SocketType::Channel,
        18 => SocketType::Peer,
        19 => SocketType::Stream,
        _ => return Err(Error::Config(format!("unknown socket type code {code}"))),
    })
}
