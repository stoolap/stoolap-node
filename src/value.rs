// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use napi::sys;
use std::ffi::CString;
use std::ptr;

use stoolap::{ParamVec, Value};

/// Check napi status and return Result.
#[inline(always)]
fn check(status: sys::napi_status) -> napi::Result<()> {
    if status == sys::Status::napi_ok {
        Ok(())
    } else {
        Err(napi::Error::new(
            napi::Status::GenericFailure,
            format!("napi call failed: {status}"),
        ))
    }
}

/// Get the JS type of a raw napi value.
fn get_type(env: sys::napi_env, val: sys::napi_value) -> napi::Result<napi::ValueType> {
    let mut val_type = 0;
    check(unsafe { sys::napi_typeof(env, val, &mut val_type) })?;
    Ok(napi::ValueType::from(val_type))
}

/// Extract a UTF-8 string from a napi string value.
/// Single allocation: Vec → truncate → String. No intermediate copy.
fn get_string(env: sys::napi_env, val: sys::napi_value) -> napi::Result<String> {
    let mut len = 0;
    check(unsafe { sys::napi_get_value_string_utf8(env, val, ptr::null_mut(), 0, &mut len) })?;
    let mut buf: Vec<u8> = vec![0u8; len + 1];
    let mut written = 0;
    check(unsafe {
        sys::napi_get_value_string_utf8(env, val, buf.as_mut_ptr().cast(), buf.len(), &mut written)
    })?;
    buf.truncate(written);
    // SAFETY: NAPI guarantees valid UTF-8 for string values
    Ok(unsafe { String::from_utf8_unchecked(buf) })
}

/// Convert a single raw JS value to a Stoolap Value.
/// Optimistic fast path: try number first (most common in query params),
/// avoiding the napi_typeof call on the hot path.
#[inline]
pub fn js_to_value(env: sys::napi_env, val: sys::napi_value) -> napi::Result<Value> {
    // Fast path: try extracting as double directly (saves 1 NAPI call for numbers)
    let mut f = 0.0;
    if unsafe { sys::napi_get_value_double(env, val, &mut f) } == sys::Status::napi_ok {
        if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            return Ok(Value::Integer(f as i64));
        }
        return Ok(Value::Float(f));
    }
    // Slow path: type-check for non-number values
    js_to_value_typed(env, val)
}

#[inline(never)]
fn js_to_value_typed(env: sys::napi_env, val: sys::napi_value) -> napi::Result<Value> {
    match get_type(env, val)? {
        napi::ValueType::Null | napi::ValueType::Undefined => Ok(Value::null_unknown()),

        napi::ValueType::Boolean => {
            let mut result = false;
            check(unsafe { sys::napi_get_value_bool(env, val, &mut result) })?;
            Ok(Value::Boolean(result))
        }

        napi::ValueType::BigInt => {
            let mut i: i64 = 0;
            let mut lossless = false;
            check(unsafe { sys::napi_get_value_bigint_int64(env, val, &mut i, &mut lossless) })?;
            if !lossless {
                return Err(napi::Error::from_reason(
                    "BigInt value is outside the range of i64",
                ));
            }
            Ok(Value::Integer(i))
        }

        napi::ValueType::String => {
            let s = get_string(env, val)?;
            Ok(Value::text(&s))
        }

        napi::ValueType::Object => {
            // Check TypedArray (Float32Array for vector params)
            let mut is_typedarray = false;
            check(unsafe { sys::napi_is_typedarray(env, val, &mut is_typedarray) })?;
            if is_typedarray {
                let mut typedarray_type = 0;
                let mut length = 0;
                let mut data = ptr::null_mut();
                let mut arraybuffer = ptr::null_mut();
                let mut offset = 0;
                check(unsafe {
                    sys::napi_get_typedarray_info(
                        env,
                        val,
                        &mut typedarray_type,
                        &mut length,
                        &mut data,
                        &mut arraybuffer,
                        &mut offset,
                    )
                })?;
                // napi_float32_array = 4
                if typedarray_type == 4 {
                    let slice =
                        unsafe { std::slice::from_raw_parts(data as *const f32, length) };
                    return Ok(Value::vector(slice.to_vec()));
                }
                return Err(napi::Error::from_reason(
                    "Only Float32Array is supported for vector parameters",
                ));
            }

            // Check Date
            let mut is_date = false;
            check(unsafe { sys::napi_is_date(env, val, &mut is_date) })?;
            if is_date {
                let mut ms = 0.0;
                check(unsafe { sys::napi_get_date_value(env, val, &mut ms) })?;
                let secs = (ms / 1000.0).floor() as i64;
                let remaining_ms = ms - (secs as f64 * 1000.0);
                let nsecs = (remaining_ms * 1_000_000.0).round() as u32;
                if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                    return Ok(Value::Timestamp(dt));
                }
                return Ok(Value::null_unknown());
            }

            // Check Buffer
            let mut is_buffer = false;
            check(unsafe { sys::napi_is_buffer(env, val, &mut is_buffer) })?;
            if is_buffer {
                let mut data = ptr::null_mut();
                let mut len = 0;
                check(unsafe { sys::napi_get_buffer_info(env, val, &mut data, &mut len) })?;
                let slice = unsafe { std::slice::from_raw_parts(data as *const u8, len) };
                let s = std::str::from_utf8(slice).map_err(|e| {
                    napi::Error::from_reason(format!("Invalid UTF-8 in Buffer: {e}"))
                })?;
                return Ok(Value::text(s));
            }

            // Plain object/array -> JSON string via JSON.stringify
            let mut global = ptr::null_mut();
            check(unsafe { sys::napi_get_global(env, &mut global) })?;
            let json_key = CString::new("JSON").unwrap();
            let mut json_obj = ptr::null_mut();
            check(unsafe {
                sys::napi_get_named_property(env, global, json_key.as_ptr(), &mut json_obj)
            })?;
            let stringify_key = CString::new("stringify").unwrap();
            let mut stringify_fn = ptr::null_mut();
            check(unsafe {
                sys::napi_get_named_property(
                    env,
                    json_obj,
                    stringify_key.as_ptr(),
                    &mut stringify_fn,
                )
            })?;
            let mut json_result = ptr::null_mut();
            let args = [val];
            check(unsafe {
                sys::napi_call_function(
                    env,
                    json_obj,
                    stringify_fn,
                    1,
                    args.as_ptr(),
                    &mut json_result,
                )
            })?;
            let s = get_string(env, json_result)?;
            Ok(Value::json(&s))
        }

        _ => Err(napi::Error::from_reason("Unsupported parameter type")),
    }
}

/// Parse a JS array directly into ParamVec (stack-allocated for ≤8 params).
/// Avoids heap allocation for queries with ≤8 parameters (the common case).
pub fn parse_positional(env: sys::napi_env, arr: sys::napi_value) -> napi::Result<ParamVec> {
    let mut is_array = false;
    check(unsafe { sys::napi_is_array(env, arr, &mut is_array) })?;
    if !is_array {
        return Err(napi::Error::from_reason(
            "Expected an array for positional parameters",
        ));
    }
    let mut len = 0u32;
    check(unsafe { sys::napi_get_array_length(env, arr, &mut len) })?;
    let mut values = ParamVec::new();
    for i in 0..len {
        let mut elem = ptr::null_mut();
        check(unsafe { sys::napi_get_element(env, arr, i, &mut elem) })?;
        values.push(js_to_value(env, elem)?);
    }
    Ok(values)
}

/// Parsed bind parameters.
pub enum BindParams {
    Positional(ParamVec),
    Named(Vec<(String, Value)>),
}

/// Opaque JS value wrapper for use as `#[napi]` function parameter.
/// Accepts any JS type without compat-mode.
pub struct RawParam(pub sys::napi_value);

impl napi::bindgen_prelude::TypeName for RawParam {
    fn type_name() -> &'static str {
        "any"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

impl napi::bindgen_prelude::ValidateNapiValue for RawParam {
    unsafe fn validate(
        _env: sys::napi_env,
        _napi_val: sys::napi_value,
    ) -> napi::Result<sys::napi_value> {
        // Accept any JS type
        Ok(std::ptr::null_mut())
    }
}

impl napi::bindgen_prelude::FromNapiValue for RawParam {
    unsafe fn from_napi_value(
        _env: sys::napi_env,
        napi_val: sys::napi_value,
    ) -> napi::Result<Self> {
        Ok(RawParam(napi_val))
    }
}

/// Parse JS params (Array or Object) into BindParams.
/// Optimistic: tries is_array first (most common), skipping typeof on the hot path.
pub fn parse_params(env: sys::napi_env, val: sys::napi_value) -> napi::Result<BindParams> {
    // Fast path: check array first (most common for prepared statements)
    let mut is_array = false;
    check(unsafe { sys::napi_is_array(env, val, &mut is_array) })?;

    if is_array {
        let mut len = 0u32;
        check(unsafe { sys::napi_get_array_length(env, val, &mut len) })?;
        let mut values = ParamVec::new();
        for i in 0..len {
            let mut elem = ptr::null_mut();
            check(unsafe { sys::napi_get_element(env, val, i, &mut elem) })?;
            values.push(js_to_value(env, elem)?);
        }
        return Ok(BindParams::Positional(values));
    }

    // Slow path: type check for null/undefined/object
    match get_type(env, val)? {
        napi::ValueType::Null | napi::ValueType::Undefined => {
            Ok(BindParams::Positional(ParamVec::new()))
        }

        napi::ValueType::Object => {
            // Plain object -> named params
            let mut keys = ptr::null_mut();
            check(unsafe { sys::napi_get_property_names(env, val, &mut keys) })?;
            let mut len = 0u32;
            check(unsafe { sys::napi_get_array_length(env, keys, &mut len) })?;
            let mut named = Vec::with_capacity(len as usize);
            for i in 0..len {
                let mut key_val = ptr::null_mut();
                check(unsafe { sys::napi_get_element(env, keys, i, &mut key_val) })?;
                let key = get_string(env, key_val)?;

                let key_cstr = CString::new(key.as_str())
                    .map_err(|e| napi::Error::from_reason(format!("Invalid key: {e}")))?;
                let mut prop_val = ptr::null_mut();
                check(unsafe {
                    sys::napi_get_named_property(env, val, key_cstr.as_ptr(), &mut prop_val)
                })?;
                let value = js_to_value(env, prop_val)?;

                // Strip leading :, @, or $ from key
                let clean = key.trim_start_matches([':', '@', '$']);
                named.push((clean.to_string(), value));
            }
            Ok(BindParams::Named(named))
        }

        _ => Err(napi::Error::from_reason(
            "Parameters must be an Array (positional) or Object (named)",
        )),
    }
}
