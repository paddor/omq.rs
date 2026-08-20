use std::ffi::{CStr, CString, c_char, c_long, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use rb_sys::{VALUE, rb_data_type_t};

pub type RbResult<T = VALUE> = Result<T, RubyErr>;

#[derive(Debug)]
pub enum RubyErr {
    Exception(VALUE),
    Error { class: VALUE, message: String },
}

impl RubyErr {
    pub fn new(class: VALUE, message: impl Into<String>) -> Self {
        Self::Error {
            class,
            message: message.into(),
        }
    }

    pub fn arg(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eArgError }, message)
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eIOError }, message)
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eRuntimeError }, message)
    }

    pub fn type_error(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eTypeError }, message)
    }

    fn current_exception() -> Self {
        let err = unsafe { rb_sys::rb_errinfo() };
        if err == qnil() {
            Self::runtime("Ruby exception")
        } else {
            Self::Exception(err)
        }
    }
}

pub fn wrap<F>(f: F) -> VALUE
where
    F: FnOnce() -> RbResult<VALUE>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => raise(err),
        Err(_) => raise(RubyErr::runtime("native Rust panic")),
    }
}

pub fn wrap_init<F>(f: F)
where
    F: FnOnce() -> RbResult<()>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => {}
        Ok(Err(err)) => raise(err),
        Err(_) => raise(RubyErr::runtime("native Rust panic")),
    }
}

pub fn raise(err: RubyErr) -> ! {
    match err {
        RubyErr::Exception(exc) => unsafe { rb_sys::rb_exc_raise(exc) },
        RubyErr::Error { class, message } => {
            let message = message.replace('\0', "\\0");
            let c_message =
                CString::new(message).unwrap_or_else(|_| CString::new("Ruby error").unwrap());
            let exc = unsafe { rb_sys::rb_exc_new_cstr(class, c_message.as_ptr()) };
            unsafe { rb_sys::rb_exc_raise(exc) }
        }
    }
}

struct ProtectData<F> {
    func: Option<F>,
    panicked: bool,
}

pub fn protect_value<F>(func: F) -> RbResult<VALUE>
where
    F: FnOnce() -> VALUE,
{
    unsafe extern "C" fn call<F>(arg: VALUE) -> VALUE
    where
        F: FnOnce() -> VALUE,
    {
        let data = unsafe { &mut *(arg as *mut ProtectData<F>) };
        let Some(func) = data.func.take() else {
            data.panicked = true;
            return qnil();
        };

        if let Ok(value) = catch_unwind(AssertUnwindSafe(func)) {
            value
        } else {
            data.panicked = true;
            qnil()
        }
    }

    let mut data = ProtectData {
        func: Some(func),
        panicked: false,
    };
    let mut state = 0;
    let value = unsafe {
        rb_sys::rb_protect(
            Some(call::<F>),
            (&raw mut data).cast::<ProtectData<F>>() as VALUE,
            &raw mut state,
        )
    };

    if state != 0 {
        Err(RubyErr::current_exception())
    } else if data.panicked {
        Err(RubyErr::runtime("native Rust panic"))
    } else {
        Ok(value)
    }
}

pub fn protect_unit<F>(func: F) -> RbResult<()>
where
    F: FnOnce(),
{
    protect_value(|| {
        func();
        qnil()
    })?;
    Ok(())
}

pub const fn qnil() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qnil as VALUE
}

pub const fn qtrue() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qtrue as VALUE
}

pub const fn qfalse() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qfalse as VALUE
}

pub const fn qundef() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qundef as VALUE
}

pub fn bool_value(value: bool) -> VALUE {
    if value { qtrue() } else { qfalse() }
}

pub fn check_hash(value: VALUE) -> RbResult<()> {
    let is_hash = unsafe { rb_sys::rb_obj_is_kind_of(value, rb_sys::rb_cHash) };
    if is_hash == qtrue() {
        Ok(())
    } else {
        Err(RubyErr::type_error("expected Hash"))
    }
}

pub fn check_array(value: VALUE) -> RbResult<()> {
    let is_array = unsafe { rb_sys::rb_obj_is_kind_of(value, rb_sys::rb_cArray) };
    if is_array == qtrue() {
        Ok(())
    } else {
        Err(RubyErr::type_error("expected Array"))
    }
}

pub fn hash_get(hash: VALUE, key: &str) -> RbResult<Option<VALUE>> {
    check_hash(hash)?;
    let key = new_utf8_string(key)?;
    let value = protect_value(|| unsafe { rb_sys::rb_hash_lookup2(hash, key, qundef()) })?;
    if value == qundef() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

pub fn hash_new() -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_hash_new() })
}

pub fn hash_aset(hash: VALUE, key: VALUE, value: VALUE) -> RbResult<()> {
    protect_value(|| unsafe { rb_sys::rb_hash_aset(hash, key, value) })?;
    Ok(())
}

pub fn array_new() -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_ary_new() })
}

pub fn array_new_capa(capacity: usize) -> RbResult<VALUE> {
    let capacity = c_long_len(capacity)?;
    protect_value(|| unsafe { rb_sys::rb_ary_new_capa(capacity) })
}

pub fn array_len(array: VALUE) -> RbResult<usize> {
    check_array(array)?;
    let len = unsafe { rb_sys::RARRAY_LEN(array) };
    if len < 0 {
        Err(RubyErr::runtime("negative Array length"))
    } else {
        Ok(len as usize)
    }
}

pub fn array_entry(array: VALUE, index: usize) -> RbResult<VALUE> {
    check_array(array)?;
    let index = c_long_len(index)?;
    protect_value(|| unsafe { rb_sys::rb_ary_entry(array, index) })
}

pub fn array_push(array: VALUE, value: VALUE) -> RbResult<()> {
    protect_value(|| unsafe { rb_sys::rb_ary_push(array, value) })?;
    Ok(())
}

pub fn symbol(name: &str) -> RbResult<VALUE> {
    let len = c_long_len(name.len())?;
    protect_value(|| unsafe {
        let id = rb_sys::rb_intern2(name.as_ptr().cast::<c_char>(), len);
        rb_sys::rb_id2sym(id)
    })
}

pub fn new_binary_string(bytes: &[u8]) -> RbResult<VALUE> {
    let len = c_long_len(bytes.len())?;
    let ptr = if bytes.is_empty() {
        ptr::null()
    } else {
        bytes.as_ptr().cast::<c_char>()
    };
    let value = protect_value(|| unsafe { rb_sys::rb_str_new(ptr, len) })?;
    protect_unit(|| unsafe { rb_sys::RB_OBJ_FREEZE(value) })?;
    Ok(value)
}

pub fn new_utf8_string(text: &str) -> RbResult<VALUE> {
    let len = c_long_len(text.len())?;
    protect_value(|| unsafe { rb_sys::rb_utf8_str_new(text.as_ptr().cast::<c_char>(), len) })
}

pub fn string_value(value: VALUE) -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_str_to_str(value) })
}

pub fn value_to_bytes(value: VALUE) -> RbResult<Vec<u8>> {
    let string = string_value(value)?;
    let len = unsafe { rb_sys::RSTRING_LEN(string) };
    if len < 0 {
        return Err(RubyErr::runtime("negative String length"));
    }
    if len == 0 {
        return Ok(Vec::new());
    }

    let ptr = unsafe { rb_sys::RSTRING_PTR(string) };
    if ptr.is_null() {
        return Err(RubyErr::runtime("null String pointer"));
    }

    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) };
    Ok(bytes.to_vec())
}

pub fn value_to_string(value: VALUE) -> RbResult<String> {
    let bytes = value_to_bytes(value)?;
    String::from_utf8(bytes).map_err(|_| RubyErr::type_error("expected UTF-8 String"))
}

pub fn value_to_i64(value: VALUE) -> RbResult<i64> {
    let mut out = 0i64;
    protect_value(|| {
        out = unsafe { rb_sys::rb_num2long(value) as i64 };
        qnil()
    })?;
    Ok(out)
}

pub fn value_to_u32(value: VALUE) -> RbResult<u32> {
    let mut out = 0;
    protect_value(|| {
        out = unsafe { rb_sys::NUM2ULONG(value) };
        qnil()
    })?;
    u32::try_from(out).map_err(|_| RubyErr::arg("routing_id must fit in a 32-bit integer"))
}

pub fn value_to_f64(value: VALUE) -> RbResult<f64> {
    let mut out = 0.0f64;
    protect_value(|| {
        out = unsafe { rb_sys::rb_num2dbl(value) };
        qnil()
    })?;
    Ok(out)
}

pub fn value_to_bool(value: VALUE) -> RbResult<bool> {
    if value == qtrue() {
        Ok(true)
    } else if value == qfalse() {
        Ok(false)
    } else {
        Err(RubyErr::type_error("expected true or false"))
    }
}

pub fn int_value(value: i32) -> VALUE {
    unsafe { rb_sys::rb_int2inum(value as isize) }
}

pub fn uint_value(value: u32) -> VALUE {
    rb_sys::ULONG2NUM(value.into())
}

pub fn u64_value(value: u64) -> VALUE {
    rb_sys::ULONG2NUM(value)
}

pub unsafe fn wrap_typed_data<T>(
    class: VALUE,
    value: Box<T>,
    data_type: *const rb_data_type_t,
) -> RbResult<VALUE> {
    let raw = Box::into_raw(value);
    match protect_value(|| unsafe {
        rb_sys::rb_data_typed_object_wrap(class, raw.cast::<c_void>(), data_type)
    }) {
        Ok(value) => Ok(value),
        Err(err) => {
            unsafe { drop(Box::from_raw(raw)) };
            Err(err)
        }
    }
}

pub unsafe fn typed_data_ref<T>(
    value: VALUE,
    data_type: *const rb_data_type_t,
    type_name: &str,
) -> RbResult<&'static T> {
    let mut ptr = std::ptr::null_mut();
    protect_unit(|| unsafe {
        ptr = rb_sys::rb_check_typeddata(value, data_type);
    })
    .map_err(|_| RubyErr::type_error(format!("expected {type_name}")))?;
    if ptr.is_null() {
        return Err(RubyErr::runtime(format!(
            "{type_name} data pointer is null"
        )));
    }

    Ok(unsafe { &*(ptr as *const T) })
}

pub unsafe fn define_module(name: &CStr) -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_define_module(name.as_ptr()) })
}

pub unsafe fn define_module_under(outer: VALUE, name: &CStr) -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_define_module_under(outer, name.as_ptr()) })
}

pub unsafe fn define_class_under(outer: VALUE, name: &CStr, superclass: VALUE) -> RbResult<VALUE> {
    protect_value(|| unsafe { rb_sys::rb_define_class_under(outer, name.as_ptr(), superclass) })
}

pub unsafe fn undef_alloc_func(class: VALUE) -> RbResult<()> {
    protect_unit(|| unsafe { rb_sys::rb_undef_alloc_func(class) })
}

pub unsafe fn define_module_function_1(
    module: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE, VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_module_function(module, name.as_ptr(), Some(transmute_1(func)), 1);
    })
}

pub unsafe fn define_module_function_0(
    module: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_module_function(module, name.as_ptr(), Some(transmute_0(func)), 0);
    })
}

pub fn call_method_1(receiver: VALUE, name: &CStr, argument: VALUE) -> RbResult<VALUE> {
    protect_value(|| unsafe {
        let method = rb_sys::rb_intern(name.as_ptr());
        rb_sys::rb_funcallv(receiver, method, 1, &raw const argument)
    })
}

pub fn call_method_0(receiver: VALUE, name: &CStr) -> RbResult<VALUE> {
    protect_value(|| unsafe {
        let method = rb_sys::rb_intern(name.as_ptr());
        rb_sys::rb_funcallv(receiver, method, 0, std::ptr::null())
    })
}

pub unsafe fn define_singleton_method_1(
    object: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE, VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_singleton_method(object, name.as_ptr(), Some(transmute_1(func)), 1);
    })
}

pub unsafe fn define_method_0(
    class: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_method(class, name.as_ptr(), Some(transmute_0(func)), 0);
    })
}

pub unsafe fn define_method_1(
    class: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE, VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_method(class, name.as_ptr(), Some(transmute_1(func)), 1);
    })
}

pub unsafe fn define_method_2(
    class: VALUE,
    name: &CStr,
    func: unsafe extern "C" fn(VALUE, VALUE, VALUE) -> VALUE,
) -> RbResult<()> {
    protect_unit(|| unsafe {
        rb_sys::rb_define_method(class, name.as_ptr(), Some(transmute_2(func)), 2);
    })
}

unsafe fn transmute_0(
    func: unsafe extern "C" fn(VALUE) -> VALUE,
) -> unsafe extern "C" fn() -> VALUE {
    unsafe {
        std::mem::transmute::<unsafe extern "C" fn(VALUE) -> VALUE, unsafe extern "C" fn() -> VALUE>(
            func,
        )
    }
}

unsafe fn transmute_1(
    func: unsafe extern "C" fn(VALUE, VALUE) -> VALUE,
) -> unsafe extern "C" fn() -> VALUE {
    unsafe {
        std::mem::transmute::<
            unsafe extern "C" fn(VALUE, VALUE) -> VALUE,
            unsafe extern "C" fn() -> VALUE,
        >(func)
    }
}

unsafe fn transmute_2(
    func: unsafe extern "C" fn(VALUE, VALUE, VALUE) -> VALUE,
) -> unsafe extern "C" fn() -> VALUE {
    unsafe {
        std::mem::transmute::<
            unsafe extern "C" fn(VALUE, VALUE, VALUE) -> VALUE,
            unsafe extern "C" fn() -> VALUE,
        >(func)
    }
}

fn c_long_len(len: usize) -> RbResult<c_long> {
    c_long::try_from(len).map_err(|_| RubyErr::arg("length too large"))
}
