#[cfg(any(feature = "curve", feature = "plain"))]
mod auth;
mod error;
mod notify;
mod options;
mod rb;
mod runtime;
mod socket;

use rb_sys::VALUE;

use crate::rb::{RbResult, RubyErr};

fn has_impl(name: VALUE) -> RbResult<VALUE> {
    let name = rb::value_to_string(name)?;
    let available = match name.as_str() {
        "ipc" | "inproc" => true,
        #[cfg(feature = "curve")]
        "curve" => true,
        #[cfg(feature = "plain")]
        "plain" => true,
        #[cfg(feature = "lz4")]
        "lz4" => true,
        #[cfg(feature = "zstd")]
        "zstd" => true,
        #[cfg(feature = "ws")]
        "ws" => true,
        _ => false,
    };
    Ok(rb::bool_value(available))
}

unsafe extern "C" fn has(_module: VALUE, name: VALUE) -> VALUE {
    rb::wrap(|| has_impl(name))
}

#[cfg(feature = "curve")]
fn curve_keypair_impl() -> RbResult<VALUE> {
    let keypair = omq_proto::CurveKeypair::generate();
    let pair = rb::array_new_capa(2)?;
    rb::array_push(
        pair,
        rb::new_binary_string(keypair.public.to_z85().as_bytes())?,
    )?;
    rb::array_push(
        pair,
        rb::new_binary_string(keypair.secret.to_z85().as_bytes())?,
    )?;
    Ok(pair)
}

#[cfg(feature = "curve")]
unsafe extern "C" fn curve_keypair(_module: VALUE) -> VALUE {
    rb::wrap(curve_keypair_impl)
}

#[cfg(feature = "curve")]
fn curve_public_impl(secret: VALUE) -> RbResult<VALUE> {
    let secret = rb::value_to_string(secret)?;
    let secret = omq_proto::CurveSecretKey::from_z85(&secret)
        .map_err(|error| RubyErr::arg(error.to_string()))?;
    rb::new_binary_string(secret.derive_public().to_z85().as_bytes())
}

#[cfg(feature = "curve")]
unsafe extern "C" fn curve_public(_module: VALUE, secret: VALUE) -> VALUE {
    rb::wrap(|| curve_public_impl(secret))
}

fn set_io_threads_impl(n: VALUE) -> RbResult<VALUE> {
    let n = rb::value_to_i64(n)?;
    if n < 0 {
        return Err(RubyErr::arg("io_threads must be non-negative"));
    }
    let n = usize::try_from(n).map_err(|_| RubyErr::arg("io_threads too large"))?;
    socket::set_io_threads(n);
    Ok(rb::qnil())
}

fn io_threads_impl() -> RbResult<VALUE> {
    let n = u64::try_from(socket::io_threads())
        .map_err(|_| RubyErr::runtime("io_threads too large"))?;
    Ok(rb::u64_value(n))
}

unsafe extern "C" fn io_threads(_module: VALUE) -> VALUE {
    rb::wrap(io_threads_impl)
}

unsafe extern "C" fn set_io_threads(_module: VALUE, n: VALUE) -> VALUE {
    rb::wrap(|| set_io_threads_impl(n))
}

#[unsafe(no_mangle)]
/// # Safety
///
/// Ruby calls this once while loading the native extension.
pub unsafe extern "C" fn Init_omq_rs_native() {
    rb::wrap_init(init);
}

fn init() -> RbResult<()> {
    #[cfg(ruby_engine = "mri")]
    unsafe {
        rb_sys::rb_ext_ractor_safe(true);
    }

    let omq = unsafe { rb::define_module(c"OMQ")? };
    let rust = unsafe { rb::define_module_under(omq, c"Rust")? };
    let native = unsafe { rb::define_module_under(rust, c"Native")? };

    unsafe {
        rb::define_module_function_0(native, c"io_threads", io_threads)?;
        rb::define_module_function_1(native, c"io_threads=", set_io_threads)?;
        rb::define_module_function_1(native, c"has", has)?;
        #[cfg(feature = "curve")]
        {
            rb::define_module_function_0(native, c"curve_keypair", curve_keypair)?;
            rb::define_module_function_1(native, c"curve_public", curve_public)?;
        }
    }

    socket::register(native)?;

    Ok(())
}
