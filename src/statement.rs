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
use napi::Env;
use std::sync::Arc;

use stoolap::api::Database;
use stoolap::{CachedPlanRef, ParamVec};

use crate::error::to_napi;
use crate::tasks::*;
use crate::value::{parse_params, parse_positional, BindParams, RawParam};

#[napi(js_name = "PreparedStatement")]
pub struct JsPreparedStatement {
    db: Arc<Database>,
    sql_text: String,
    plan: CachedPlanRef,
}

impl JsPreparedStatement {
    pub fn new(db: Arc<Database>, sql: String) -> napi::Result<Self> {
        let plan = db.cached_plan(&sql).map_err(to_napi)?;
        Ok(Self {
            db,
            sql_text: sql,
            plan,
        })
    }
}

#[napi]
impl JsPreparedStatement {
    /// Execute the statement (DML). Returns Promise<{ changes: number }>.
    #[napi(
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Promise<RunResult>"
    )]
    pub fn execute(&self, env: Env, params: Option<RawParam>) -> napi::Result<AsyncTask<ExecTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(ExecTask {
            db: Arc::clone(&self.db),
            sql: self.sql_text.clone(),
            params: task_params,
            plan: Some(self.plan.clone()),
        }))
    }

    /// Query rows. Returns Promise<Array<Object>>.
    #[napi(
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Promise<Record<string, any>[]>"
    )]
    pub fn query(&self, env: Env, params: Option<RawParam>) -> napi::Result<AsyncTask<QueryTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryTask {
            db: Arc::clone(&self.db),
            sql: self.sql_text.clone(),
            params: task_params,
            plan: Some(self.plan.clone()),
        }))
    }

    /// Query single row. Returns Promise<Object | null>.
    #[napi(
        js_name = "queryOne",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Promise<Record<string, any> | null>"
    )]
    pub fn query_one(
        &self,
        env: Env,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<QueryOneTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryOneTask {
            db: Arc::clone(&self.db),
            sql: self.sql_text.clone(),
            params: task_params,
            plan: Some(self.plan.clone()),
        }))
    }

    /// Query rows in raw format. Returns Promise<{ columns: string[], rows: any[][] }>.
    #[napi(
        js_name = "queryRaw",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Promise<{ columns: string[], rows: any[][] }>"
    )]
    pub fn query_raw(
        &self,
        env: Env,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<QueryRawTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryRawTask {
            db: Arc::clone(&self.db),
            sql: self.sql_text.clone(),
            params: task_params,
            plan: Some(self.plan.clone()),
        }))
    }

    // ================================================================
    // Synchronous methods
    // ================================================================

    /// Execute synchronously. Returns { changes: number }.
    #[napi(
        js_name = "executeSync",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "RunResult"
    )]
    pub fn execute_sync(&self, env: Env, params: Option<RawParam>) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&env, params)?;
        let changes = task_params.execute_plan_on_db(&self.db, &self.plan)?;
        Ok(RawJsValue(v8_run_result(changes)))
    }

    /// Query rows synchronously. Returns Array<Object>.
    /// Uses direct V8 bulk object creation — bypasses NAPI per-property overhead.
    #[napi(
        js_name = "querySync",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Record<string, any>[]"
    )]
    pub fn query_sync(&self, _env: Env, params: Option<RawParam>) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = task_params.query_plan_on_db(&self.db, &self.plan)?;
        Ok(RawJsValue(v8_streaming_rows_to_array(rows)))
    }

    /// Query single row synchronously. Returns Object | null.
    /// Uses direct V8 bulk object creation — optimal hidden class in one call.
    #[napi(
        js_name = "queryOneSync",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "Record<string, any> | null"
    )]
    pub fn query_one_sync(&self, _env: Env, params: Option<RawParam>) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = task_params.query_plan_on_db(&self.db, &self.plan)?;
        Ok(RawJsValue(v8_single_row_or_null(rows)))
    }

    /// Query rows in raw format synchronously. Returns { columns: string[], rows: any[][] }.
    /// Uses direct V8 bulk array creation — bypasses NAPI per-element overhead.
    #[napi(
        js_name = "queryRawSync",
        ts_args_type = "params?: any[] | Record<string, any>",
        ts_return_type = "{ columns: string[], rows: any[][] }"
    )]
    pub fn query_raw_sync(&self, _env: Env, params: Option<RawParam>) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = task_params.query_plan_on_db(&self.db, &self.plan)?;
        Ok(RawJsValue(v8_streaming_rows_to_raw(rows)))
    }

    /// Execute the prepared SQL with multiple param sets in a single call.
    /// Uses pre-cached AST, auto-wraps in a transaction: begin, execute all, commit.
    /// Returns { changes: total_rows_affected }.
    #[napi(
        js_name = "executeBatchSync",
        ts_args_type = "paramsArray: any[][]",
        ts_return_type = "RunResult"
    )]
    pub fn execute_batch_sync(&self, env: Env, params_array: RawParam) -> napi::Result<RawJsValue> {
        use napi::sys;
        let raw_env = env.raw();
        let arr = params_array.0;

        let mut is_array = false;
        check(unsafe { sys::napi_is_array(raw_env, arr, &mut is_array) })?;
        if !is_array {
            return Err(napi::Error::from_reason("paramsArray must be an array"));
        }

        let mut len = 0u32;
        check(unsafe { sys::napi_get_array_length(raw_env, arr, &mut len) })?;

        // Use pre-cached AST from the plan (no re-parsing)
        let stmt = self.plan.statement.as_ref();

        let mut tx = self.db.begin().map_err(to_napi)?;
        let mut total_changes = 0i64;

        for i in 0..len {
            let mut elem = std::ptr::null_mut();
            check(unsafe { sys::napi_get_element(raw_env, arr, i, &mut elem) })?;
            let params = parse_positional(raw_env, elem)?;
            total_changes += tx.execute_prepared(stmt, params).map_err(to_napi)?;
        }

        tx.commit().map_err(to_napi)?;
        Ok(RawJsValue(v8_run_result(total_changes)))
    }

    /// Get the SQL text of this prepared statement.
    #[napi(getter)]
    pub fn sql(&self) -> String {
        self.sql_text.clone()
    }
}

fn convert_params(env: &Env, params: Option<RawParam>) -> napi::Result<TaskParams> {
    match params {
        None => Ok(TaskParams::Positional(ParamVec::new())),
        Some(p) => match parse_params(env.raw(), p.0)? {
            BindParams::Positional(pos) => Ok(TaskParams::Positional(pos)),
            BindParams::Named(n) => Ok(TaskParams::Named(n)),
        },
    }
}
