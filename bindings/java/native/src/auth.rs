//! Authentication callbacks, credentials, and key conversion.

#[cfg(feature = "plain")]
use crate::convert::java_strings;
#[cfg(any(feature = "plain", feature = "curve"))]
use crate::convert::{curve_key_z85, java_string, optional_java_string};
#[cfg(any(feature = "plain", feature = "curve"))]
use crate::error::jni_error;
use crate::error::{guard, throw_omq};
#[cfg(any(feature = "plain", feature = "curve"))]
use crate::socket::socket_from_handle;
use jni::JNIEnv;
#[cfg(any(feature = "plain", feature = "curve"))]
use jni::objects::JValue;
use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jlong, jobjectArray, jstring};
use omq_tokio::Error;
#[cfg(any(feature = "plain", feature = "curve"))]
use omq_tokio::{Authenticator, MechanismPeerInfo, MechanismSetup};
#[cfg(feature = "curve")]
use omq_tokio::{CurveKeypair, CurvePublicKey, CurveSecretKey, CurveServerOptions};

#[cfg(any(feature = "plain", feature = "curve"))]
fn mechanism_peer_info_object<'local>(
    env: &mut JNIEnv<'local>,
    peer: &MechanismPeerInfo,
) -> Result<JObject<'local>, Error> {
    let mechanism = env
        .new_string(peer.mechanism.as_str()?.to_string())
        .map_err(jni_error)?;
    let public_key = if peer.mechanism == omq_proto::proto::MechanismName::CURVE {
        optional_java_string(env, curve_key_z85(&peer.public_key).as_deref())?
    } else {
        JObject::null()
    };
    let identity = match &peer.identity {
        Some(identity) => JObject::from(env.byte_array_from_slice(identity).map_err(jni_error)?),
        None => JObject::null(),
    };
    let peer_address = match &peer.peer_address {
        Some(address) => JObject::from(env.new_string(address).map_err(jni_error)?),
        None => JObject::null(),
    };
    let username = match &peer.username {
        Some(username) => JObject::from(env.new_string(username).map_err(jni_error)?),
        None => JObject::null(),
    };
    let password = match &peer.password {
        Some(password) => JObject::from(env.new_string(password).map_err(jni_error)?),
        None => JObject::null(),
    };

    env.new_object(
        "io/omq/PeerInfo",
        "(Ljava/lang/String;Ljava/lang/String;[BLjava/lang/String;Ljava/lang/String;Ljava/lang/String;)V",
        &[
            JValue::Object(&mechanism),
            JValue::Object(&public_key),
            JValue::Object(&identity),
            JValue::Object(&username),
            JValue::Object(&password),
            JValue::Object(&peer_address),
        ],
    )
    .map_err(jni_error)
}

#[cfg(any(feature = "plain", feature = "curve"))]
fn java_authenticator(
    env: &mut JNIEnv<'_>,
    authenticator: JObject<'_>,
) -> Result<Authenticator, Error> {
    if authenticator.is_null() {
        return Err(Error::Config("authenticator must not be null".to_string()));
    }
    let jvm = env.get_java_vm().map_err(jni_error)?;
    let authenticator = env.new_global_ref(authenticator).map_err(jni_error)?;
    Ok(Authenticator::new(move |peer| {
        let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
            return false;
        };
        let Ok(info) = mechanism_peer_info_object(&mut env, peer) else {
            return false;
        };
        match env.call_method(
            &authenticator,
            "test",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&info)],
        ) {
            Ok(value) => value.z().unwrap_or(false),
            Err(_) => {
                let _ = env.exception_clear();
                false
            }
        }
    }))
}

#[cfg(feature = "curve")]
fn curve_keypair_from_z85(public_key: String, secret_key: String) -> Result<CurveKeypair, Error> {
    let public = CurvePublicKey::from_z85(&public_key)?;
    let secret = CurveSecretKey::from_z85(&secret_key)?;
    Ok(CurveKeypair { public, secret })
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "system" fn Java_io_omq_Native_curveKeypair(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let keypair = CurveKeypair::generate();
            let string_class = env.find_class("java/lang/String").map_err(jni_error)?;
            let out = env
                .new_object_array(2, string_class, JObject::null())
                .map_err(jni_error)?;
            let public = env.new_string(keypair.public.to_z85()).map_err(jni_error)?;
            let secret = env.new_string(keypair.secret.to_z85()).map_err(jni_error)?;
            env.set_object_array_element(&out, 0, public)
                .map_err(jni_error)?;
            env.set_object_array_element(&out, 1, secret)
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
#[cfg(feature = "curve")]
pub extern "system" fn Java_io_omq_Native_curvePublic(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    secret_key: JString<'_>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let result = (|| {
            let secret = CurveSecretKey::from_z85(&java_string(env, secret_key)?)?;
            env.new_string(secret.derive_public().to_z85())
                .map(|s| s.into_raw())
                .map_err(jni_error)
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
#[cfg(feature = "plain")]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    usernames: JObjectArray<'_>,
    passwords: JObjectArray<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let usernames = java_strings(env, &usernames)?;
            let passwords = java_strings(env, &passwords)?;
            if usernames.len() != passwords.len() {
                return Err(Error::Config(
                    "PLAIN username/password array lengths differ".into(),
                ));
            }
            let expected = usernames.into_iter().zip(passwords);
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainServer {
                    authenticator: Authenticator::plain_credentials(expected),
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let authenticator = java_authenticator(env, authenticator)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainServer { authenticator };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
#[cfg(feature = "plain")]
pub extern "system" fn Java_io_omq_Native_socketSetPlainClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    username: JString<'_>,
    password: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let username = java_string(env, username)?;
            let password = java_string(env, password)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::PlainClient { username, password };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::CurveServer {
                    our_keypair: keypair,
                    options: CurveServerOptions::default(),
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
    authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            let authenticator = java_authenticator(env, authenticator)?;
            socket.set_option(move |options| {
                let mut curve_options = CurveServerOptions::default();
                curve_options.authenticator = Some(authenticator);
                options.mechanism = MechanismSetup::CurveServer {
                    our_keypair: keypair,
                    options: curve_options,
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[unsafe(no_mangle)]
#[cfg(feature = "curve")]
pub extern "system" fn Java_io_omq_Native_socketSetCurveClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    public_key: JString<'_>,
    secret_key: JString<'_>,
    server_public_key: JString<'_>,
) {
    guard(&mut env, (), |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let keypair = curve_keypair_from_z85(
                java_string(env, public_key)?,
                java_string(env, secret_key)?,
            )?;
            let server_public = CurvePublicKey::from_z85(&java_string(env, server_public_key)?)?;
            socket.set_option(move |options| {
                options.mechanism = MechanismSetup::CurveClient {
                    our_keypair: keypair,
                    server_public,
                };
            })
        })();

        if let Err(error) = result {
            throw_omq(env, error);
        }
    });
}

#[cfg(not(all(feature = "plain", feature = "curve")))]
fn throw_disabled(env: &mut JNIEnv<'_>, mechanism: &str) {
    throw_omq(
        env,
        Error::Config(format!("{mechanism} support not enabled")),
    );
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "system" fn Java_io_omq_Native_curveKeypair(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
) -> jobjectArray {
    guard(&mut env, std::ptr::null_mut(), |env| {
        throw_disabled(env, "CURVE");
        std::ptr::null_mut()
    })
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "system" fn Java_io_omq_Native_curvePublic(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _secret_key: JString<'_>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        throw_disabled(env, "CURVE");
        std::ptr::null_mut()
    })
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _usernames: JObjectArray<'_>,
    _passwords: JObjectArray<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "PLAIN"));
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "system" fn Java_io_omq_Native_socketSetPlainServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "PLAIN"));
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "plain"))]
pub extern "system" fn Java_io_omq_Native_socketSetPlainClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _username: JString<'_>,
    _password: JString<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "PLAIN"));
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServer(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _public_key: JString<'_>,
    _secret_key: JString<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "CURVE"));
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "system" fn Java_io_omq_Native_socketSetCurveServerCallback(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _public_key: JString<'_>,
    _secret_key: JString<'_>,
    _authenticator: JObject<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "CURVE"));
}

#[unsafe(no_mangle)]
#[cfg(not(feature = "curve"))]
pub extern "system" fn Java_io_omq_Native_socketSetCurveClient(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    _handle: jlong,
    _public_key: JString<'_>,
    _secret_key: JString<'_>,
    _server_public_key: JString<'_>,
) {
    guard(&mut env, (), |env| throw_disabled(env, "CURVE"));
}
