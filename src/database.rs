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
use stoolap::ParamVec;

use crate::error::to_napi;
use crate::statement::JsPreparedStatement;
use crate::tasks::*;
use crate::value::{parse_params, parse_positional, BindParams, RawParam};

#[napi(js_name = "Database")]
pub struct JsDatabase {
    db: Arc<Database>,
}

impl JsDatabase {
    pub fn from_db(db: Database) -> Self {
        Self { db: Arc::new(db) }
    }
}

#[napi]
impl JsDatabase {
    /// Open a database. Returns a Promise that resolves to a Database instance.
    ///
    /// Accepts:
    /// - `:memory:` or empty string for in-memory database
    /// - `memory://` for in-memory database
    /// - `file:///path/to/db` for file-based database
    /// - Bare path like `./mydb` for file-based database
    #[napi(ts_return_type = "Promise<Database>")]
    pub fn open(path: String) -> AsyncTask<OpenTask> {
        let dsn = translate_path(&path);
        AsyncTask::new(OpenTask { dsn })
    }

    /// Execute a DDL/DML statement. Returns Promise<{ changes: number }>.
    ///
    /// @param sql - SQL statement
    /// @param params - Optional: Array for positional ($1, $2) or Object for named (:key)
    #[napi(
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<RunResult>"
    )]
    pub fn execute(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<ExecTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(ExecTask {
            db: Arc::clone(&self.db),
            sql,
            params: task_params,
            plan: None,
        }))
    }

    /// Execute one or more SQL statements separated by semicolons.
    /// Returns Promise<void>.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn exec(&self, sql: String) -> AsyncTask<BatchExecTask> {
        AsyncTask::new(BatchExecTask {
            db: Arc::clone(&self.db),
            sql,
        })
    }

    /// Query rows. Returns Promise<Array<Object>>.
    ///
    /// Each row is an object with column names as keys.
    #[napi(
        ts_args_type = "sql: string, params?: any[] | Record<string, any>",
        ts_return_type = "Promise<Record<string, any>[]>"
    )]
    pub fn query(
        &self,
        env: Env,
        sql: String,
        params: Option<RawParam>,
    ) -> napi::Result<AsyncTask<QueryTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryTask {
            db: Arc::clone(&self.db),
            sql,
            params: task_params,
            plan: None,
        }))
    }

    /// Query a single row. Returns Promise<Object | null>.
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
    ) -> napi::Result<AsyncTask<QueryOneTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryOneTask {
            db: Arc::clone(&self.db),
            sql,
            params: task_params,
            plan: None,
        }))
    }

    /// Query rows in raw format. Returns Promise<{ columns: string[], rows: any[][] }>.
    ///
    /// Faster than query() — skips per-row object creation.
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
    ) -> napi::Result<AsyncTask<QueryRawTask>> {
        let task_params = convert_params(&env, params)?;
        Ok(AsyncTask::new(QueryRawTask {
            db: Arc::clone(&self.db),
            sql,
            params: task_params,
            plan: None,
        }))
    }

    // ================================================================
    // Synchronous methods — no Promise overhead, runs on main thread
    // ================================================================

    /// Execute a DML statement synchronously. Returns { changes: number }.
    ///
    /// Faster than execute() for simple operations — no async overhead.
    /// Blocks the event loop, so use for fast operations only.
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
        let changes = task_params.execute_on_db(&self.db, &sql)?;
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
        let rows = task_params.query_on_db(&self.db, &sql)?;
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
        let rows = task_params.query_on_db(&self.db, &sql)?;
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
        let rows = task_params.query_on_db(&self.db, &sql)?;
        Ok(RawJsValue(v8_streaming_rows_to_raw(rows)))
    }

    // ================================================================
    // Other sync helpers
    // ================================================================

    /// Execute the same SQL with multiple param sets in a single call.
    /// Parses SQL once, auto-wraps in a transaction: begin, execute all, commit.
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

    /// Execute one or more SQL statements synchronously.
    #[napi(js_name = "execSync")]
    pub fn exec_sync(&self, sql: String) -> napi::Result<()> {
        for stmt in crate::tasks::split_sql_statements(&sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            self.db.execute(trimmed, ()).map_err(to_napi)?;
        }
        Ok(())
    }

    /// Create a prepared statement (synchronous — parses and caches the plan).
    #[napi]
    pub fn prepare(&self, sql: String) -> napi::Result<JsPreparedStatement> {
        JsPreparedStatement::new(Arc::clone(&self.db), sql)
    }

    /// Begin a transaction. Returns Promise<Transaction>.
    #[napi(ts_return_type = "Promise<Transaction>")]
    pub fn begin(&self) -> AsyncTask<BeginTask> {
        AsyncTask::new(BeginTask {
            db: Arc::clone(&self.db),
        })
    }

    /// Begin a transaction synchronously. Returns Transaction.
    #[napi(js_name = "beginSync", ts_return_type = "Transaction")]
    pub fn begin_sync(&self) -> napi::Result<crate::transaction::JsTransaction> {
        let tx = self.db.begin().map_err(to_napi)?;
        Ok(crate::transaction::JsTransaction::from_tx(tx))
    }

    /// Close the database. Returns Promise<void>.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn close(&self) -> AsyncTask<CloseTask> {
        AsyncTask::new(CloseTask {
            db: Arc::clone(&self.db),
        })
    }
}

/// Translate user-friendly paths to Stoolap DSN format.
fn translate_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == ":memory:" {
        "memory://".to_string()
    } else if trimmed.starts_with("memory://") || trimmed.starts_with("file://") {
        trimmed.to_string()
    } else {
        // Bare file path
        format!("file://{trimmed}")
    }
}

/// Convert JS params to TaskParams.
fn convert_params(env: &Env, params: Option<RawParam>) -> napi::Result<TaskParams> {
    match params {
        None => Ok(TaskParams::Positional(ParamVec::new())),
        Some(p) => match parse_params(env.raw(), p.0)? {
            BindParams::Positional(pos) => Ok(TaskParams::Positional(pos)),
            BindParams::Named(n) => Ok(TaskParams::Named(n)),
        },
    }
}
