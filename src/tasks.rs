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

use napi::bindgen_prelude::*;
use napi::{sys, Env, Task};
use std::ptr;
use std::sync::{Arc, Mutex};

use stoolap::api::Database;
use stoolap::api::NamedParams;
use stoolap::api::Transaction as ApiTransaction;
use stoolap::{CachedPlanRef, ParamVec, Value};

// ============================================================
// V8 bulk object creation via C++ FFI
// ============================================================

/// Cell type tags — must match C++ CellTag enum
const TAG_NULL: u8 = 0;
const TAG_BOOL_FALSE: u8 = 1;
const TAG_BOOL_TRUE: u8 = 2;
const TAG_INT32: u8 = 3;
const TAG_DOUBLE: u8 = 4;
const TAG_STRING: u8 = 5;
const TAG_INT64: u8 = 6;

/// C-compatible cell data — must match C++ CellData layout exactly.
/// Passed to V8 helper for direct value creation (bypasses NAPI).
#[repr(C)]
struct CellData {
    tag: u8,
    // 7 bytes padding (automatic with repr(C))
    int_val: i64,
    float_val: f64,
    str_ptr: *const u8,
    str_len: i32,
    // 4 bytes padding (automatic with repr(C))
}

/// Callback type for streaming row creation.
/// C++ calls this per row; returns 1 if row available, 0 when done.
type RowCallback = extern "C" fn(ctx: *mut std::ffi::c_void, cells: *mut CellData) -> i32;

extern "C" {
    fn v8_create_single_object(
        col_count: i32,
        col_ptrs: *const *const u8,
        col_lens: *const i32,
        cells: *const CellData,
    ) -> sys::napi_value;

    fn v8_create_null() -> sys::napi_value;

    fn v8_create_rows_streaming(
        col_count: i32,
        col_ptrs: *const *const u8,
        col_lens: *const i32,
        next_row: RowCallback,
        ctx: *mut std::ffi::c_void,
    ) -> sys::napi_value;

    fn v8_create_raw_streaming(
        col_count: i32,
        col_ptrs: *const *const u8,
        col_lens: *const i32,
        next_row: RowCallback,
        ctx: *mut std::ffi::c_void,
    ) -> sys::napi_value;

    fn v8_create_run_result(changes: i64) -> sys::napi_value;
}

/// Context passed to the streaming callback.
/// Holds a raw pointer to Rows (valid for the duration of the C++ call).
struct StreamContext {
    rows: *mut stoolap::Rows,
    temp_strings: Vec<String>,
    col_count: usize,
}

/// Streaming callback: advance Rows, fill CellData directly from current_row().
/// No Value cloning — reads borrowed references.
extern "C" fn stream_next_row(ctx: *mut std::ffi::c_void, cells: *mut CellData) -> i32 {
    let ctx = unsafe { &mut *(ctx as *mut StreamContext) };
    // Clear temp strings from previous row (Timestamp formatting)
    ctx.temp_strings.clear();

    let rows = unsafe { &mut *ctx.rows };
    if !rows.advance() {
        return 0;
    }

    let values = rows.current_row().as_slice();
    for (i, val) in values.iter().enumerate().take(ctx.col_count) {
        unsafe {
            *cells.add(i) = value_to_cell(val, &mut ctx.temp_strings);
        }
    }
    1
}

/// Convert a stoolap Value to CellData for V8 bulk creation.
/// For Timestamp values, the formatted string is pushed to `temp_strings`
/// (the caller must keep temp_strings alive until the C++ call completes).
#[inline]
fn value_to_cell(val: &Value, temp_strings: &mut Vec<String>) -> CellData {
    match val {
        Value::Null(_) => CellData {
            tag: TAG_NULL,
            int_val: 0,
            float_val: 0.0,
            str_ptr: ptr::null(),
            str_len: 0,
        },
        Value::Boolean(false) => CellData {
            tag: TAG_BOOL_FALSE,
            int_val: 0,
            float_val: 0.0,
            str_ptr: ptr::null(),
            str_len: 0,
        },
        Value::Boolean(true) => CellData {
            tag: TAG_BOOL_TRUE,
            int_val: 0,
            float_val: 0.0,
            str_ptr: ptr::null(),
            str_len: 0,
        },
        Value::Integer(i) => {
            let i = *i;
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                CellData {
                    tag: TAG_INT32,
                    int_val: i,
                    float_val: 0.0,
                    str_ptr: ptr::null(),
                    str_len: 0,
                }
            } else {
                CellData {
                    tag: TAG_INT64,
                    int_val: i,
                    float_val: 0.0,
                    str_ptr: ptr::null(),
                    str_len: 0,
                }
            }
        }
        Value::Float(f) => {
            if f.is_nan() || f.is_infinite() {
                CellData {
                    tag: TAG_NULL,
                    int_val: 0,
                    float_val: 0.0,
                    str_ptr: ptr::null(),
                    str_len: 0,
                }
            } else {
                CellData {
                    tag: TAG_DOUBLE,
                    int_val: 0,
                    float_val: *f,
                    str_ptr: ptr::null(),
                    str_len: 0,
                }
            }
        }
        Value::Text(s) => {
            let s_ref: &str = s;
            CellData {
                tag: TAG_STRING,
                int_val: 0,
                float_val: 0.0,
                str_ptr: s_ref.as_ptr(),
                str_len: s_ref.len() as i32,
            }
        }
        Value::Timestamp(ts) => {
            use chrono::{Datelike, Timelike};
            let mut s = String::with_capacity(22);
            let mut b = itoa::Buffer::new();
            let y = ts.year();
            if (0..10).contains(&y) {
                s.push_str("000");
            } else if (10..100).contains(&y) {
                s.push_str("00");
            } else if (100..1000).contains(&y) {
                s.push('0');
            }
            s.push_str(b.format(y));
            s.push('-');
            let m = ts.month();
            if m < 10 {
                s.push('0');
            }
            s.push_str(b.format(m));
            s.push('-');
            let d = ts.day();
            if d < 10 {
                s.push('0');
            }
            s.push_str(b.format(d));
            s.push('T');
            let h = ts.hour();
            if h < 10 {
                s.push('0');
            }
            s.push_str(b.format(h));
            s.push(':');
            let min = ts.minute();
            if min < 10 {
                s.push('0');
            }
            s.push_str(b.format(min));
            s.push(':');
            let sec = ts.second();
            if sec < 10 {
                s.push('0');
            }
            s.push_str(b.format(sec));
            s.push('Z');
            // Push to temp_strings; String's heap buffer won't move on Vec realloc
            temp_strings.push(s);
            let last = temp_strings.last().unwrap();
            CellData {
                tag: TAG_STRING,
                int_val: 0,
                float_val: 0.0,
                str_ptr: last.as_ptr(),
                str_len: last.len() as i32,
            }
        }
        Value::Json(s) => {
            let s_ref: &str = s.as_ref();
            CellData {
                tag: TAG_STRING,
                int_val: 0,
                float_val: 0.0,
                str_ptr: s_ref.as_ptr(),
                str_len: s_ref.len() as i32,
            }
        }
    }
}

/// Collected rows for async path — transfer from compute() to resolve().
pub struct CollectedRows {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
}

/// Context for streaming over already-collected rows (async resolve path).
struct CollectedStreamContext<'a> {
    data: &'a CollectedRows,
    row_idx: usize,
    temp_strings: Vec<String>,
}

/// Streaming callback for collected rows: iterates over Vec<Vec<Value>> row by row.
/// Avoids allocating a flat Vec<CellData> for all rows — reuses C++ per-row buffer.
extern "C" fn collected_next_row(ctx: *mut std::ffi::c_void, cells: *mut CellData) -> i32 {
    let ctx = unsafe { &mut *(ctx as *mut CollectedStreamContext) };
    if ctx.row_idx >= ctx.data.rows.len() {
        return 0;
    }
    ctx.temp_strings.clear();
    let row = &ctx.data.rows[ctx.row_idx];
    for (i, val) in row.iter().enumerate() {
        unsafe {
            *cells.add(i) = value_to_cell(val, &mut ctx.temp_strings);
        }
    }
    ctx.row_idx += 1;
    1
}

/// Convert collected rows to a JS array using V8 streaming callback.
/// Iterates row-by-row over the collected data — no flat CellData allocation.
fn collected_rows_to_v8_array(data: &CollectedRows) -> sys::napi_value {
    let col_count = data.columns.len();

    let col_ptrs: Vec<*const u8> = data.columns.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<i32> = data.columns.iter().map(|c| c.len() as i32).collect();

    let mut ctx = CollectedStreamContext {
        data,
        row_idx: 0,
        temp_strings: Vec::new(),
    };

    unsafe {
        v8_create_rows_streaming(
            col_count as i32,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            collected_next_row,
            &mut ctx as *mut CollectedStreamContext as *mut std::ffi::c_void,
        )
    }
}

/// Create a JS array of row objects from streaming Rows using V8 callback API.
/// Zero-copy: C++ calls back into Rust per row, reading directly from current_row().
/// No Vec<Vec<Value>> collection, no Value cloning.
pub(crate) fn v8_streaming_rows_to_array(mut rows: stoolap::Rows) -> sys::napi_value {
    let columns = rows.columns().to_vec();
    let col_count = columns.len();

    let col_ptrs: Vec<*const u8> = columns.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<i32> = columns.iter().map(|c| c.len() as i32).collect();

    let mut ctx = StreamContext {
        rows: &mut rows as *mut _,
        temp_strings: Vec::new(),
        col_count,
    };

    unsafe {
        v8_create_rows_streaming(
            col_count as i32,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            stream_next_row,
            &mut ctx as *mut StreamContext as *mut std::ffi::c_void,
        )
    }
}

/// Create a single JS object or null from streaming Rows using V8 bulk API.
pub(crate) fn v8_single_row_or_null(mut rows: stoolap::Rows) -> sys::napi_value {
    if !rows.advance() {
        return unsafe { v8_create_null() };
    }

    let columns = rows.columns().to_vec();
    let col_count = columns.len();
    let values = rows.current_row().as_slice();

    let col_ptrs: Vec<*const u8> = columns.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<i32> = columns.iter().map(|c| c.len() as i32).collect();

    let mut temp_strings: Vec<String> = Vec::new();
    let cells: Vec<CellData> = values
        .iter()
        .map(|v| value_to_cell(v, &mut temp_strings))
        .collect();

    unsafe {
        v8_create_single_object(
            col_count as i32,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            cells.as_ptr(),
        )
    }
}

/// Create a raw-format JS object { columns: string[], rows: any[][] } from streaming Rows.
/// Zero-copy sync path using V8 callback API.
pub(crate) fn v8_streaming_rows_to_raw(mut rows: stoolap::Rows) -> sys::napi_value {
    let columns = rows.columns().to_vec();
    let col_count = columns.len();

    let col_ptrs: Vec<*const u8> = columns.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<i32> = columns.iter().map(|c| c.len() as i32).collect();

    let mut ctx = StreamContext {
        rows: &mut rows as *mut _,
        temp_strings: Vec::new(),
        col_count,
    };

    unsafe {
        v8_create_raw_streaming(
            col_count as i32,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            stream_next_row,
            &mut ctx as *mut StreamContext as *mut std::ffi::c_void,
        )
    }
}

/// Convert collected rows to a raw-format JS object using V8 streaming callback.
/// Used by async QueryRawTask resolve path.
fn collected_rows_to_v8_raw(data: &CollectedRows) -> sys::napi_value {
    let col_count = data.columns.len();

    let col_ptrs: Vec<*const u8> = data.columns.iter().map(|c| c.as_ptr()).collect();
    let col_lens: Vec<i32> = data.columns.iter().map(|c| c.len() as i32).collect();

    let mut ctx = CollectedStreamContext {
        data,
        row_idx: 0,
        temp_strings: Vec::new(),
    };

    unsafe {
        v8_create_raw_streaming(
            col_count as i32,
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            collected_next_row,
            &mut ctx as *mut CollectedStreamContext as *mut std::ffi::c_void,
        )
    }
}

/// Collect all rows into CollectedRows for async transfer.
fn collect_all_rows(mut rows: stoolap::Rows) -> CollectedRows {
    let columns = rows.columns().to_vec();
    let mut collected = Vec::new();
    while rows.advance() {
        collected.push(rows.current_row().as_slice().to_vec());
    }
    CollectedRows {
        columns,
        rows: collected,
    }
}

/// Collect single row data for async transfer.
fn collect_single_row_data(mut rows: stoolap::Rows) -> Option<CollectedRows> {
    if !rows.advance() {
        return None;
    }
    let columns = rows.columns().to_vec();
    let values = rows.current_row().as_slice().to_vec();
    Some(CollectedRows {
        columns,
        rows: vec![values],
    })
}

/// Convert a single CollectedRows (with one row) to a V8 object, or null if None.
/// Shared by QueryOneTask and TxQueryOneTask resolve paths.
fn collected_single_row_to_v8(data: Option<CollectedRows>) -> sys::napi_value {
    match data {
        Some(data) => {
            let col_count = data.columns.len();
            let col_ptrs: Vec<*const u8> = data.columns.iter().map(|c| c.as_ptr()).collect();
            let col_lens: Vec<i32> = data.columns.iter().map(|c| c.len() as i32).collect();
            let mut temp_strings: Vec<String> = Vec::new();
            let cells: Vec<CellData> = data.rows[0]
                .iter()
                .map(|v| value_to_cell(v, &mut temp_strings))
                .collect();
            unsafe {
                v8_create_single_object(
                    col_count as i32,
                    col_ptrs.as_ptr(),
                    col_lens.as_ptr(),
                    cells.as_ptr(),
                )
            }
        }
        None => unsafe { v8_create_null() },
    }
}

/// Shared database handle — Arc::clone (not Database::clone) to share executor & cache.
pub type DbHandle = Arc<Database>;

use crate::error::to_napi;

// ============================================================
// RawJsValue — newtype for Task::JsValue (heterogeneous JS values)
// ============================================================

pub struct RawJsValue(pub sys::napi_value);

impl TypeName for RawJsValue {
    fn type_name() -> &'static str {
        "Unknown"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

impl ToNapiValue for RawJsValue {
    unsafe fn to_napi_value(_env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        Ok(val.0)
    }
}

// ============================================================
// RunResult — { changes: number } via V8 bulk API
// ============================================================

/// Create a `{ changes: N }` JS object using V8 bulk API (1 call vs 3 NAPI calls).
pub(crate) fn v8_run_result(changes: i64) -> sys::napi_value {
    unsafe { v8_create_run_result(changes) }
}

// ============================================================
// Raw NAPI helpers — zero overhead
// ============================================================

#[inline(always)]
pub(crate) fn check(status: sys::napi_status) -> napi::Result<()> {
    if status == sys::Status::napi_ok {
        Ok(())
    } else {
        Err(napi::Error::new(
            napi::Status::GenericFailure,
            format!("napi call failed: {status}"),
        ))
    }
}

// ============================================================
// Execute parameters enum
// ============================================================

pub enum TaskParams {
    Positional(ParamVec),
    Named(Vec<(String, Value)>),
}

impl TaskParams {
    pub(crate) fn execute_on_db(self, db: &Database, sql: &str) -> napi::Result<i64> {
        match self {
            TaskParams::Positional(p) => db.execute(sql, p).map_err(to_napi),
            TaskParams::Named(n) => {
                let mut named = NamedParams::new();
                for (k, v) in n {
                    named.insert(k, v);
                }
                db.execute_named(sql, named).map_err(to_napi)
            }
        }
    }

    pub(crate) fn query_on_db(self, db: &Database, sql: &str) -> napi::Result<stoolap::Rows> {
        match self {
            TaskParams::Positional(p) => db.query(sql, p).map_err(to_napi),
            TaskParams::Named(n) => {
                let mut named = NamedParams::new();
                for (k, v) in n {
                    named.insert(k, v);
                }
                db.query_named(sql, named).map_err(to_napi)
            }
        }
    }

    pub(crate) fn execute_plan_on_db(
        &self,
        db: &Database,
        plan: &CachedPlanRef,
    ) -> napi::Result<i64> {
        match self {
            TaskParams::Positional(p) => db.execute_plan(plan, p.clone()).map_err(to_napi),
            TaskParams::Named(n) => {
                let mut named = NamedParams::new();
                for (k, v) in n {
                    named.insert(k.clone(), v.clone());
                }
                db.execute_named_plan(plan, named).map_err(to_napi)
            }
        }
    }

    pub(crate) fn query_plan_on_db(
        &self,
        db: &Database,
        plan: &CachedPlanRef,
    ) -> napi::Result<stoolap::Rows> {
        match self {
            TaskParams::Positional(p) => db.query_plan(plan, p.clone()).map_err(to_napi),
            TaskParams::Named(n) => {
                let mut named = NamedParams::new();
                for (k, v) in n {
                    named.insert(k.clone(), v.clone());
                }
                db.query_named_plan(plan, named).map_err(to_napi)
            }
        }
    }

    pub(crate) fn execute_on_tx(self, tx: &mut ApiTransaction, sql: &str) -> napi::Result<i64> {
        match self {
            TaskParams::Positional(p) => tx.execute(sql, p).map_err(to_napi),
            TaskParams::Named(_) => Err(napi::Error::from_reason(
                "Named parameters not yet supported in transactions",
            )),
        }
    }

    pub(crate) fn query_on_tx(
        self,
        tx: &mut ApiTransaction,
        sql: &str,
    ) -> napi::Result<stoolap::Rows> {
        match self {
            TaskParams::Positional(p) => tx.query(sql, p).map_err(to_napi),
            TaskParams::Named(_) => Err(napi::Error::from_reason(
                "Named parameters not yet supported in transactions",
            )),
        }
    }
}

// ============================================================
// OpenTask — Database.open()
// ============================================================

pub struct OpenTask {
    pub dsn: String,
}

impl Task for OpenTask {
    type Output = Database;
    type JsValue = crate::database::JsDatabase;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        Database::open(&self.dsn).map_err(to_napi)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(crate::database::JsDatabase::from_db(output))
    }
}

// ============================================================

// ExecTask — db.execute(sql, params)
// ============================================================

pub struct ExecTask {
    pub db: DbHandle,
    pub sql: String,
    pub params: TaskParams,
    pub plan: Option<CachedPlanRef>,
}

impl Task for ExecTask {
    type Output = i64;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        if let Some(ref plan) = self.plan {
            params.execute_plan_on_db(&self.db, plan)
        } else {
            params.execute_on_db(&self.db, &self.sql)
        }
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(v8_run_result(output)))
    }
}

// ============================================================
// BatchExecTask — db.exec(sql)
// ============================================================

pub struct BatchExecTask {
    pub db: DbHandle,
    pub sql: String,
}

impl Task for BatchExecTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        for stmt in split_sql_statements(&self.sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.db.execute(trimmed, ()).map_err(to_napi)?;
        }
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

// ============================================================
// QueryTask — db.query(sql, params) -> array of objects
// ============================================================

pub struct QueryTask {
    pub db: DbHandle,
    pub sql: String,
    pub params: TaskParams,
    pub plan: Option<CachedPlanRef>,
}

impl Task for QueryTask {
    type Output = CollectedRows;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = if let Some(ref plan) = self.plan {
            params.query_plan_on_db(&self.db, plan)?
        } else {
            params.query_on_db(&self.db, &self.sql)?
        };
        Ok(collect_all_rows(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_rows_to_v8_array(&output)))
    }
}

// ============================================================
// QueryRawTask — db.queryRaw(sql, params) -> { columns, rows }
// ============================================================

pub struct QueryRawTask {
    pub db: DbHandle,
    pub sql: String,
    pub params: TaskParams,
    pub plan: Option<CachedPlanRef>,
}

impl Task for QueryRawTask {
    type Output = CollectedRows;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = if let Some(ref plan) = self.plan {
            params.query_plan_on_db(&self.db, plan)?
        } else {
            params.query_on_db(&self.db, &self.sql)?
        };
        Ok(collect_all_rows(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_rows_to_v8_raw(&output)))
    }
}

// ============================================================
// QueryOneTask — db.queryOne(sql, params) -> object | null
// ============================================================

pub struct QueryOneTask {
    pub db: DbHandle,
    pub sql: String,
    pub params: TaskParams,
    pub plan: Option<CachedPlanRef>,
}

impl Task for QueryOneTask {
    type Output = Option<CollectedRows>;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = if let Some(ref plan) = self.plan {
            params.query_plan_on_db(&self.db, plan)?
        } else {
            params.query_on_db(&self.db, &self.sql)?
        };
        Ok(collect_single_row_data(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_single_row_to_v8(output)))
    }
}

// ============================================================
// CloseTask — db.close()
// ============================================================

pub struct CloseTask {
    pub db: DbHandle,
}

impl Task for CloseTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.db.close().map_err(to_napi)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

// ============================================================
// BeginTask — db.begin()
// ============================================================

pub struct BeginTask {
    pub db: DbHandle,
}

impl Task for BeginTask {
    type Output = ApiTransaction;
    type JsValue = crate::transaction::JsTransaction;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        self.db.begin().map_err(to_napi)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(crate::transaction::JsTransaction::from_tx(output))
    }
}

// ============================================================
// Transaction tasks
// ============================================================

pub type TxHandle = Arc<Mutex<Option<ApiTransaction>>>;

fn with_tx<F, R>(handle: &TxHandle, f: F) -> napi::Result<R>
where
    F: FnOnce(&mut ApiTransaction) -> napi::Result<R>,
{
    let mut guard = handle
        .lock()
        .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
    let tx = guard
        .as_mut()
        .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
    f(tx)
}

fn take_tx(handle: &TxHandle) -> napi::Result<ApiTransaction> {
    let mut guard = handle
        .lock()
        .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
    guard
        .take()
        .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))
}

// TxExecTask

pub struct TxExecTask {
    pub tx: TxHandle,
    pub sql: String,
    pub params: TaskParams,
}

impl Task for TxExecTask {
    type Output = i64;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        with_tx(&self.tx, |tx| params.execute_on_tx(tx, &self.sql))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(v8_run_result(output)))
    }
}

// TxQueryTask

pub struct TxQueryTask {
    pub tx: TxHandle,
    pub sql: String,
    pub params: TaskParams,
}

impl Task for TxQueryTask {
    type Output = CollectedRows;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = with_tx(&self.tx, |tx| params.query_on_tx(tx, &self.sql))?;
        Ok(collect_all_rows(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_rows_to_v8_array(&output)))
    }
}

// TxQueryOneTask

pub struct TxQueryOneTask {
    pub tx: TxHandle,
    pub sql: String,
    pub params: TaskParams,
}

impl Task for TxQueryOneTask {
    type Output = Option<CollectedRows>;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = with_tx(&self.tx, |tx| params.query_on_tx(tx, &self.sql))?;
        Ok(collect_single_row_data(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_single_row_to_v8(output)))
    }
}

// TxQueryRawTask

pub struct TxQueryRawTask {
    pub tx: TxHandle,
    pub sql: String,
    pub params: TaskParams,
}

impl Task for TxQueryRawTask {
    type Output = CollectedRows;
    type JsValue = RawJsValue;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let params = std::mem::replace(&mut self.params, TaskParams::Positional(ParamVec::new()));
        let rows = with_tx(&self.tx, |tx| params.query_on_tx(tx, &self.sql))?;
        Ok(collect_all_rows(rows))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(RawJsValue(collected_rows_to_v8_raw(&output)))
    }
}

// CommitTask

pub struct CommitTask {
    pub tx: TxHandle,
}

impl Task for CommitTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let mut tx = take_tx(&self.tx)?;
        tx.commit().map_err(to_napi)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

// RollbackTask

pub struct RollbackTask {
    pub tx: TxHandle,
}

impl Task for RollbackTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let mut tx = take_tx(&self.tx)?;
        tx.rollback().map_err(to_napi)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(())
    }
}

// ============================================================
// Helpers
// ============================================================

/// Split SQL statements by semicolons, respecting quotes and comments.
pub(crate) fn split_sql_statements(input: &str) -> Vec<&str> {
    let mut stmts = Vec::new();
    let mut start = 0;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let bytes = input.as_bytes();
    let len = bytes.len();

    let mut i = 0;
    while i < len {
        let b = bytes[i];

        if in_line_comment {
            if b == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }

        if in_block_comment {
            if b == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }

        if in_single {
            if b == b'\'' {
                in_single = false;
            }
            i += 1;
            continue;
        }

        if in_double {
            if b == b'"' {
                in_double = false;
            }
            i += 1;
            continue;
        }

        // Outside quotes and comments
        if b == b'\'' {
            in_single = true;
        } else if b == b'"' {
            in_double = true;
        } else if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 2;
            continue;
        } else if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            in_block_comment = true;
            i += 2;
            continue;
        } else if b == b';' {
            let s = &input[start..i];
            if !s.trim().is_empty() {
                stmts.push(s);
            }
            start = i + 1;
        }

        i += 1;
    }
    let s = &input[start..];
    if !s.trim().is_empty() {
        stmts.push(s);
    }
    stmts
}
