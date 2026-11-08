//! Java identity handles and peer metadata.

use crate::convert::{optional_java_bytes, optional_java_string};
use crate::error::{guard, jni_error, throw_omq};
use jni::JNIEnv;
use jni::objects::{JClass, JObject, JValue};
use jni::sys::{jboolean, jint, jlong, jobject};
use omq_tokio::{CurvePublicKey, Error, PeerInfo as NativePeerInfo};

pub(crate) fn peer_info_object<'local>(
    env: &mut JNIEnv<'local>,
    peer: &NativePeerInfo,
) -> Result<JObject<'local>, Error> {
    let identity = match &peer.peer_identity {
        Some(identity) => JObject::from(env.byte_array_from_slice(identity).map_err(jni_error)?),
        None => JObject::null(),
    };
    let peer_address = match peer.peer_address {
        Some(address) => JObject::from(env.new_string(address.to_string()).map_err(jni_error)?),
        None => JObject::null(),
    };
    let socket_type = match peer.peer_properties.socket_type {
        Some(socket_type) => JObject::from(
            env.new_string(format!("{socket_type:?}"))
                .map_err(jni_error)?,
        ),
        None => JObject::null(),
    };

    let authentication = peer_authentication_object(env, &peer.authentication)?;
    let diagnostics = peer_diagnostics_object(env, &peer.diagnostics)?;
    let mechanism = optional_java_string(
        env,
        peer.authentication
            .mechanism
            .as_ref()
            .and_then(|m| m.as_str().ok()),
    )?;
    let public_key = match &peer.authentication.public_key {
        Some(omq_proto::PeerPublicKey::Curve(key)) => {
            optional_java_string(env, Some(&CurvePublicKey::from_bytes(*key).to_z85()))?
        }
        _ => JObject::null(),
    };
    let null = JObject::null();
    env.new_object(
        "io/omq/PeerInfo",
        "(Ljava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;IILio/omq/PeerAuthentication;Lio/omq/PeerDiagnostics;)V",
        &[
            JValue::Object(&mechanism),
            JValue::Object(&public_key),
            JValue::Object(&identity),
            JValue::Object(&null),
            JValue::Object(&null),
            JValue::Long(peer.connection_id as jlong),
            JValue::Object(&peer_address),
            JValue::Object(&socket_type),
            JValue::Int(peer.zmtp_version.0 as jint),
            JValue::Int(peer.zmtp_version.1 as jint),
            JValue::Object(&authentication),
            JValue::Object(&diagnostics),
        ],
    )
    .map_err(jni_error)
}

fn peer_authentication_object<'local>(
    env: &mut JNIEnv<'local>,
    auth: &omq_proto::PeerAuthentication,
) -> Result<JObject<'local>, Error> {
    let mechanism =
        optional_java_string(env, auth.mechanism.as_ref().and_then(|m| m.as_str().ok()))?;
    let (key_type, key) = match &auth.public_key {
        Some(omq_proto::PeerPublicKey::Curve(key)) => (Some("CURVE"), Some(key.as_slice())),
        _ => (None, None),
    };
    let key_type = optional_java_string(env, key_type)?;
    let key = optional_java_bytes(env, key)?;
    let user_id = optional_java_bytes(env, auth.user_id.as_deref())?;
    let metadata = env
        .new_object("java/util/LinkedHashMap", "()V", &[])
        .map_err(jni_error)?;
    for (name, value) in &auth.metadata {
        env.with_local_frame(8, |env| -> jni::errors::Result<()> {
            let name = JObject::from(env.new_string(name)?);
            let value = JObject::from(env.byte_array_from_slice(value)?);
            env.call_method(
                &metadata,
                "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&name), JValue::Object(&value)],
            )?;
            Ok(())
        })
        .map_err(jni_error)?;
    }
    env.new_object(
        "io/omq/PeerAuthentication",
        "(Ljava/lang/String;Ljava/lang/String;[B[BLjava/util/Map;)V",
        &[
            JValue::Object(&mechanism),
            JValue::Object(&key_type),
            JValue::Object(&key),
            JValue::Object(&user_id),
            JValue::Object(&metadata),
        ],
    )
    .map_err(jni_error)
}

fn peer_diagnostics_object<'local>(
    env: &mut JNIEnv<'local>,
    diagnostics: &omq_proto::PeerDiagnostics,
) -> Result<JObject<'local>, Error> {
    let endpoint = optional_java_string(env, Some(&diagnostics.endpoint))?;
    let local = optional_java_string(
        env,
        diagnostics
            .local_address
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    )?;
    let remote = optional_java_string(
        env,
        diagnostics
            .remote_address
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
    )?;
    env.new_object(
        "io/omq/PeerDiagnostics",
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&endpoint),
            JValue::Object(&local),
            JValue::Object(&remote),
        ],
    )
    .map_err(jni_error)
}

pub(crate) fn identity_object<'local>(
    env: &mut JNIEnv<'local>,
    identity: &omq_proto::Identity,
) -> Result<JObject<'local>, Error> {
    let bytes = JObject::from(
        env.byte_array_from_slice(identity.as_ref())
            .map_err(jni_error)?,
    );
    let handle = Box::into_raw(Box::new(identity.clone()));
    match env.new_object(
        "io/omq/Identity",
        "(J[B)V",
        &[JValue::Long(handle as jlong), JValue::Object(&bytes)],
    ) {
        Ok(object) => Ok(object),
        Err(error) => {
            // Construction did not register a Cleaner; reclaim the native owner.
            unsafe {
                drop(Box::from_raw(handle));
            }
            Err(jni_error(error))
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_identityRelease(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    // Only the Cleaner registered for this owner calls release, exactly once.
    if handle != 0 {
        unsafe {
            drop(Box::from_raw(handle as *mut omq_proto::Identity));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_identityPeerInfo(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) -> jobject {
    guard(&mut env, std::ptr::null_mut(), |env| {
        // Java keeps the Identity reachable until this call completes.
        let identity = unsafe { &*(handle as *const omq_proto::Identity) };
        match peer_info_object(env, identity.peer_info()) {
            Ok(info) => info.into_raw(),
            Err(error) => {
                throw_omq(env, error);
                std::ptr::null_mut()
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_identityEquals(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    first: jlong,
    second: jlong,
) -> jboolean {
    // Both Java owners are kept reachable by the caller.
    let (first, second) = unsafe {
        (
            &*(first as *const omq_proto::Identity),
            &*(second as *const omq_proto::Identity),
        )
    };
    u8::from(first == second)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_identityHash(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) -> jint {
    use std::hash::{Hash, Hasher};
    let identity = unsafe { &*(handle as *const omq_proto::Identity) };
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    identity.hash(&mut hash);
    hash.finish() as jint
}
