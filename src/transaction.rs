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
use std::sync::{Arc, Mutex};

use stoolap::api::Transaction as ApiTransaction;
use stoolap::ParamVec;

use crate::error::to_napi;
use crate::tasks::*;
use crate::value::{parse_params, parse_positional, BindParams, RawParam};

#[napi(js_name = "Transaction")]
pub struct JsTransaction {
    tx: TxHandle,
}

impl JsTransaction {
    pub fn from_tx(tx: ApiTransaction) -> Self {
        Self {
            tx: Arc::new(Mutex::new(Some(tx))),
        }
    }
}

#[napi]
impl JsTransaction {
    // ================================================================
    // Async methods (run on libuv worker thread)
    // ================================================================

    /// Execute a DML statement within the transaction.
    /// Returns Promise<{ changes: number }>.
    #[napi(
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<RunResult>"
    )]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<TxExecTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(TxExecTask {
            tx: self.tx.clone(),
            sql,
            params: task_params,
        }))
    }

    /// Query rows within the transaction.
    /// Returns Promise<Array<Object>>.
    #[napi(
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<Record<string, any>[]>"
    )]
    pub fn query(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<TxQueryTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(TxQueryTask {
            tx: self.tx.clone(),
            sql,
            params: task_params,
        }))
    }

    /// Query a single row within the transaction.
    /// Returns Promise<Object | null>.
    #[napi(
        js_name = "queryOne",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<Record<string, any> | null>"
    )]
    pub fn query_one(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<TxQueryOneTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(TxQueryOneTask {
            tx: self.tx.clone(),
            sql,
            params: task_params,
        }))
    }

    /// Query rows in raw format within the transaction.
    /// Returns Promise<{ columns: string[], rows: any[][] }>.
    #[napi(
        js_name = "queryRaw",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<{ columns: string[], rows: any[][] }>"
    )]
    pub fn query_raw(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<TxQueryRawTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(TxQueryRawTask {
            tx: self.tx.clone(),
            sql,
            params: task_params,
        }))
    }

    /// Commit the transaction. Returns Promise<void>.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn commit(&self) -> AsyncTask<CommitTask> {
        AsyncTask::new(CommitTask {
            tx: self.tx.clone(),
        })
    }

    /// Rollback the transaction. Returns Promise<void>.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn rollback(&self) -> AsyncTask<RollbackTask> {
        AsyncTask::new(RollbackTask {
            tx: self.tx.clone(),
        })
    }

    // ================================================================
    // Synchronous methods — no Promise overhead, runs on main thread
    // ================================================================

    /// Execute a DML statement synchronously. Returns { changes: number }.
    #[napi(
        js_name = "executeSync",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "RunResult"
    )]
    pub fn execute_sync(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&env, params)?;
        let changes = {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
            let tx = guard
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
            task_params.execute_on_tx(tx, &sql)?
        };
        Ok(RawJsValue(v8_run_result(changes)))
    }

    /// Query rows synchronously. Returns Array<Object>.
    /// Uses direct V8 bulk object creation — bypasses NAPI per-property overhead.
    #[napi(
        js_name = "querySync",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Record<string, any>[]"
    )]
    pub fn query_sync(
        &self,
        _env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
            let tx = guard
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
            task_params.query_on_tx(tx, &sql)?
        };
        Ok(RawJsValue(v8_streaming_rows_to_array(rows)))
    }

    /// Query a single row synchronously. Returns Object | null.
    /// Uses direct V8 bulk object creation — optimal hidden class in one call.
    #[napi(
        js_name = "queryOneSync",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Record<string, any> | null"
    )]
    pub fn query_one_sync(
        &self,
        _env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
            let tx = guard
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
            task_params.query_on_tx(tx, &sql)?
        };
        Ok(RawJsValue(v8_single_row_or_null(rows)))
    }

    /// Query rows in raw format synchronously. Returns { columns: string[], rows: any[][] }.
    /// Uses direct V8 bulk array creation — bypasses NAPI per-element overhead.
    #[napi(
        js_name = "queryRawSync",
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "{ columns: string[], rows: any[][] }"
    )]
    pub fn query_raw_sync(
        &self,
        _env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<RawJsValue> {
        let task_params = convert_params(&_env, params)?;
        let rows = {
            let mut guard = self
                .tx
                .lock()
                .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
            let tx = guard
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
            task_params.query_on_tx(tx, &sql)?
        };
        Ok(RawJsValue(v8_streaming_rows_to_raw(rows)))
    }

    /// Commit the transaction synchronously.
    #[napi(js_name = "commitSync")]
    pub fn commit_sync(&self) -> napi::Result<()> {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
        let mut tx = guard
            .take()
            .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
        tx.commit().map_err(to_napi)
    }

    /// Execute the same SQL with multiple param sets in a single call.
    /// Parses SQL once, locks the transaction once — eliminates per-call overhead.
    /// Returns { changes: total_rows_affected }.
    #[napi(
        js_name = "executeBatchSync",
        ts_args_type = "sql: string, paramsArray: any[][]",
        ts_return_type = "RunResult"
    )]
    pub fn execute_batch_sync(
        &self,
        env: Env,
        sql: String,
        params_array: RawParam,
    ) -> napi::Result<RawJsValue> {
        use napi::sys;
        use stoolap::parser::Parser;
        let raw_env = env.raw();
        let arr = params_array.0;

        let mut is_array = false;
        check(unsafe { sys::napi_is_array(raw_env, arr, &mut is_array) })?;
        if !is_array {
            return Err(napi::Error::from_reason("paramsArray must be an array"));
        }

        let mut len = 0u32;
        check(unsafe { sys::napi_get_array_length(raw_env, arr, &mut len) })?;

        // Parse SQL once for all executions
        let mut parser = Parser::new(&sql);
        let program = parser
            .parse_program()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let stmt = program
            .statements
            .first()
            .ok_or_else(|| napi::Error::from_reason("No SQL statement found"))?;

        let mut guard = self
            .tx
            .lock()
            .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
        let tx = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;

        let mut total_changes = 0i64;
        for i in 0..len {
            let mut elem = std::ptr::null_mut();
            check(unsafe { sys::napi_get_element(raw_env, arr, i, &mut elem) })?;
            let params = parse_positional(raw_env, elem)?;
            total_changes += tx.execute_prepared(stmt, params).map_err(to_napi)?;
        }

        Ok(RawJsValue(v8_run_result(total_changes)))
    }

    /// Rollback the transaction synchronously.
    #[napi(js_name = "rollbackSync")]
    pub fn rollback_sync(&self) -> napi::Result<()> {
        let mut guard = self
            .tx
            .lock()
            .map_err(|_| napi::Error::from_reason("Transaction lock poisoned"))?;
        let mut tx = guard
            .take()
            .ok_or_else(|| napi::Error::from_reason("Transaction is no longer active"))?;
        tx.rollback().map_err(to_napi)
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
