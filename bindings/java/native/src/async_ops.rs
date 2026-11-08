//! Java future completion and asynchronous operations.

use crate::convert::optional_duration_from_millis;
use crate::error::{exception_object, guard, jni_error, runtime_exception_object, throw_omq};
use crate::message::{
    bytes_from_parts, java_message, message_to_java_object, received_message_object,
};
use crate::socket::recv::receive_any_loop;
use crate::socket::socket_from_handle;
use jni::objects::{GlobalRef, JClass, JLongArray, JObject, JObjectArray, JValue};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use omq_tokio::blocking::Socket as BlockingSocket;
use omq_tokio::{Error, Message};

struct JavaAsyncTask {
    abort: tokio::task::AbortHandle,
}

fn async_task_handle(join: tokio::task::JoinHandle<()>) -> jlong {
    let abort = join.abort_handle();
    Box::into_raw(Box::new(JavaAsyncTask { abort })) as jlong
}

fn complete_future_exceptionally(env: &mut JNIEnv<'_>, future: &GlobalRef, error: Error) {
    let throwable = exception_object(env, error)
        .or_else(|_| runtime_exception_object(env, "failed to create native OMQ exception"));
    if let Ok(throwable) = throwable {
        let _ = env.call_method(
            future,
            "completeExceptionally",
            "(Ljava/lang/Throwable;)Z",
            &[JValue::Object(&throwable)],
        );
    }
}

fn complete_future_message(jvm: JavaVM, future: GlobalRef, result: Result<Message, Error>) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(message) => match message_to_java_object(&mut env, message) {
            Ok(message) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&message)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn complete_future_received_message(
    jvm: JavaVM,
    future: GlobalRef,
    result: Result<Message, Error>,
) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    let _ = env.with_local_frame(32, |env| -> jni::errors::Result<()> {
        match result {
            Ok(message) => match received_message_object(env, message) {
                Ok(message) => {
                    let _ = env.call_method(
                        &future,
                        "complete",
                        "(Ljava/lang/Object;)Z",
                        &[JValue::Object(&message)],
                    );
                }
                Err(error) => complete_future_exceptionally(env, &future, error),
            },
            Err(error) => complete_future_exceptionally(env, &future, error),
        }
        Ok(())
    });
}

fn receive_event_object<'local>(
    env: &mut JNIEnv<'local>,
    socket: &GlobalRef,
    message: Message,
) -> Result<JObject<'local>, Error> {
    let message = message_to_java_object(env, message)?;
    let socket = env.new_local_ref(socket.as_obj()).map_err(jni_error)?;
    env.new_object(
        "io/omq/ReceiveEvent",
        "(Lio/omq/Socket;Lio/omq/Message;)V",
        &[JValue::Object(&socket), JValue::Object(&message)],
    )
    .map_err(jni_error)
}

fn complete_future_receive_event(
    jvm: JavaVM,
    future: GlobalRef,
    result: Result<(GlobalRef, Message), Error>,
) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok((socket, message)) => match receive_event_object(&mut env, &socket, message) {
            Ok(event) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&event)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn optional_receive_event_object<'local>(
    env: &mut JNIEnv<'local>,
    event: Option<(GlobalRef, Message)>,
) -> Result<JObject<'local>, Error> {
    match event {
        Some((socket, message)) => {
            let event = receive_event_object(env, &socket, message)?;
            env.call_static_method(
                "java/util/Optional",
                "of",
                "(Ljava/lang/Object;)Ljava/util/Optional;",
                &[JValue::Object(&event)],
            )
            .and_then(|value| value.l())
            .map_err(jni_error)
        }
        None => env
            .call_static_method("java/util/Optional", "empty", "()Ljava/util/Optional;", &[])
            .and_then(|value| value.l())
            .map_err(jni_error),
    }
}

fn complete_future_optional_receive_event(
    jvm: JavaVM,
    future: GlobalRef,
    result: Result<Option<(GlobalRef, Message)>, Error>,
) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(event) => match optional_receive_event_object(&mut env, event) {
            Ok(event) => {
                let _ = env.call_method(
                    &future,
                    "complete",
                    "(Ljava/lang/Object;)Z",
                    &[JValue::Object(&event)],
                );
            }
            Err(error) => complete_future_exceptionally(&mut env, &future, error),
        },
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

fn complete_future_void(jvm: JavaVM, future: GlobalRef, result: Result<(), Error>) {
    let Ok(mut env) = jvm.attach_current_thread_as_daemon() else {
        return;
    };
    match result {
        Ok(()) => {
            let value = JObject::null();
            let _ = env.call_method(
                &future,
                "complete",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&value)],
            );
        }
        Err(error) => complete_future_exceptionally(&mut env, &future, error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_asyncTaskCancel(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
) {
    guard(&mut env, (), |_env| {
        if handle == 0 {
            return;
        }
        let task = unsafe { Box::from_raw(handle as *mut JavaAsyncTask) };
        task.abort.abort();
    });
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_receiveAnyAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    sockets: JObjectArray<'_>,
    handles: JLongArray<'_>,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let len = env.get_array_length(&sockets).map_err(jni_error)?;
            if len <= 0 {
                return Err(Error::Config("at least one socket is required".to_string()));
            }
            if env.get_array_length(&handles).map_err(jni_error)? != len {
                return Err(Error::Config("socket and handle arrays differ".to_string()));
            }
            let mut raw_handles = vec![0; len as usize];
            env.get_long_array_region(&handles, 0, &mut raw_handles)
                .map_err(jni_error)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let mut entries = Vec::with_capacity(len as usize);
            for i in 0..len {
                let socket_obj = env
                    .get_object_array_element(&sockets, i)
                    .map_err(jni_error)?;
                if socket_obj.is_null() {
                    return Err(Error::Config(format!("socket {i} must not be null")));
                }
                let handle = raw_handles[i as usize];
                let socket = socket_from_handle(handle)?;
                let java_socket = env.new_global_ref(&socket_obj).map_err(jni_error)?;
                let runtime = socket.context().handle().clone();
                let socket = socket.materialize()?;
                entries.push((runtime, socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let entries: Vec<(BlockingSocket, GlobalRef)> = entries
                .into_iter()
                .map(|(_runtime, socket, java_socket)| (socket, java_socket))
                .collect();
            let join = parent_runtime.spawn(async move {
                let result = match receive_any_loop(entries, None).await {
                    Ok(Some(event)) => Ok(event),
                    Ok(None) => Err(Error::Closed),
                    Err(error) => Err(error),
                };
                complete_future_receive_event(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_receiveAnyAsyncOptional(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    sockets: JObjectArray<'_>,
    handles: JLongArray<'_>,
    timeout_millis: jlong,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let timeout = optional_duration_from_millis(timeout_millis)?;
            let len = env.get_array_length(&sockets).map_err(jni_error)?;
            if len <= 0 {
                return Err(Error::Config("at least one socket is required".to_string()));
            }
            if env.get_array_length(&handles).map_err(jni_error)? != len {
                return Err(Error::Config("socket and handle arrays differ".to_string()));
            }
            let mut raw_handles = vec![0; len as usize];
            env.get_long_array_region(&handles, 0, &mut raw_handles)
                .map_err(jni_error)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let mut entries = Vec::with_capacity(len as usize);
            for i in 0..len {
                let socket_obj = env
                    .get_object_array_element(&sockets, i)
                    .map_err(jni_error)?;
                if socket_obj.is_null() {
                    return Err(Error::Config(format!("socket {i} must not be null")));
                }
                let handle = raw_handles[i as usize];
                let socket = socket_from_handle(handle)?;
                let java_socket = env.new_global_ref(&socket_obj).map_err(jni_error)?;
                let runtime = socket.context().handle().clone();
                let socket = socket.materialize()?;
                entries.push((runtime, socket, java_socket));
            }

            let parent_runtime = entries[0].0.clone();
            let entries: Vec<(BlockingSocket, GlobalRef)> = entries
                .into_iter()
                .map(|(_runtime, socket, java_socket)| (socket, java_socket))
                .collect();
            let join = parent_runtime.spawn(async move {
                let result = receive_any_loop(entries, timeout).await;
                complete_future_optional_receive_event(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketSendAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    parts: JObjectArray<'_>,
    routing_id: jint,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let parts = bytes_from_parts(env, parts)?;
            let message = java_message(parts, routing_id)?;
            let handle = socket.context().handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = socket.send(message).await;
                complete_future_void(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecvAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(timeout_millis)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let handle = socket.context().handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = match timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, socket.recv()).await {
                        Ok(result) => result,
                        Err(_) => Err(Error::Timeout),
                    },
                    None => socket.recv().await,
                };
                complete_future_message(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_omq_Native_socketRecvFromAsync(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    handle: jlong,
    timeout_millis: jlong,
    future: JObject<'_>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let result = (|| {
            let socket = socket_from_handle(handle)?;
            let timeout = optional_duration_from_millis(timeout_millis)?;
            let jvm = env.get_java_vm().map_err(jni_error)?;
            let future = env.new_global_ref(&future).map_err(jni_error)?;
            let handle = socket.context().handle().clone();
            let socket = socket.materialize()?.into_async();
            let join = handle.spawn(async move {
                let result = match timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, socket.recv()).await {
                        Ok(result) => result,
                        Err(_) => Err(Error::Timeout),
                    },
                    None => socket.recv().await,
                };
                complete_future_received_message(jvm, future, result);
            });
            Ok(async_task_handle(join))
        })();

        match result {
            Ok(handle) => handle,
            Err(error) => {
                throw_omq(env, error);
                0
            }
        }
    })
}
