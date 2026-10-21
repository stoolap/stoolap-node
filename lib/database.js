"use strict";

const {
  loadStoolap,
  processParams,
  rewriteNamedParams,
  splitNamed,
} = require("./ffi");
const { parseRows, parseRaw } = require("./parse");
const { Transaction } = require("./transaction");
const { PreparedStatement, encodeBatch } = require("./statement");
const { RowCursor } = require("./iterator");

function normalizeDsn(dbPath) {
  let dsn = dbPath || ":memory:";
  if (
    dsn !== ":memory:" &&
    dsn !== "memory://" &&
    dsn !== "" &&
    !dsn.includes("://")
  ) {
    dsn = `file://${dsn}`;
  }
  return dsn;
}

class Database {
  #ptr = null;
  #stoolap = null;
  #pendingAsync = 0;
  #closeWaiter = null;
  #closingPromise = null;
  #children = new Set();
  #cursors = new Set(); /* WeakRef<RowCursor> */

  constructor(ptr, stoolap) {
    this.#ptr = ptr;
    this.#stoolap = stoolap;
  }

  _trackChild(child) {
    this.#children.add(child);
  }

  _untrackChild(child) {
    this.#children.delete(child);
  }

  _trackCursor(ref) {
    this.#cursors.add(ref);
  }

  _untrackCursor(ref) {
    this.#cursors.delete(ref);
  }

  #closeChildren() {
    for (const ref of this.#cursors) {
      const cursor = ref.deref();
      if (cursor) cursor._invalidate();
    }
    this.#cursors.clear();
    for (const child of this.#children) {
      child._invalidate();
    }
    this.#children.clear();
  }

  #childrenPending() {
    for (const child of this.#children) {
      if (child._hasPending && child._hasPending()) return true;
    }
    return false;
  }

  static async open(dbPath) {
    const stoolap = loadStoolap();
    const ptr = await stoolap.dbOpenAsync(normalizeDsn(dbPath));
    return new Database(ptr, stoolap);
  }

  static openSync(dbPath) {
    const stoolap = loadStoolap();
    const ptr = stoolap.dbOpen(normalizeDsn(dbPath));
    return new Database(ptr, stoolap);
  }

  #checkOpen() {
    if (!this.#ptr) throw new Error("Database is closed");
  }

  #notifyCloseWaiter() {
    if (this.#pendingAsync === 0 && this.#closeWaiter) {
      const resolve = this.#closeWaiter;
      this.#closeWaiter = null;
      resolve();
    }
  }

  async #runAsync(operation) {
    this.#checkOpen();
    if (this.#closingPromise) throw new Error("Database is closing");
    const ptr = this.#ptr;
    this.#pendingAsync += 1;
    try {
      return await operation(ptr);
    } finally {
      this.#pendingAsync -= 1;
      this.#notifyCloseWaiter();
    }
  }

  _executeAsync(sql, params) {
    return this.#runAsync((ptr) =>
      params === undefined
        ? this.#stoolap.dbExecAsync(ptr, sql)
        : this.#stoolap.dbExecAsync(ptr, sql, params),
    );
  }

  _queryBufferAsync(sql, params) {
    return this.#runAsync((ptr) =>
      params === undefined
        ? this.#stoolap.dbQueryBufAsync(ptr, sql)
        : this.#stoolap.dbQueryBufAsync(ptr, sql, params),
    );
  }

  _execAsync(sql) {
    return this.#runAsync((ptr) => this.#stoolap.dbExecSimpleAsync(ptr, sql));
  }

  _beginAsync() {
    return this.#runAsync((ptr) => this.#stoolap.txBeginAsync(ptr));
  }

  _stmtExecAsync(stmtPtr, params) {
    return this.#runAsync(() =>
      params === undefined
        ? this.#stoolap.stmtExecAsync(stmtPtr)
        : this.#stoolap.stmtExecAsync(stmtPtr, params),
    );
  }

  _stmtQueryBufAsync(stmtPtr, params) {
    return this.#runAsync(() =>
      params === undefined
        ? this.#stoolap.stmtQueryBufAsync(stmtPtr)
        : this.#stoolap.stmtQueryBufAsync(stmtPtr, params),
    );
  }


  _rowsChunk(rowsPtr, maxRows, buf) {
    this.#checkOpen();
    return this.#stoolap.rowsChunk(rowsPtr, maxRows, buf);
  }

  _rowsChunkAsync(rowsPtr, maxRows) {
    return this.#runAsync(() => this.#stoolap.rowsChunkAsync(rowsPtr, maxRows));
  }

  _executeBatchBufferSync(sql, batchBuf) {
    this.#checkOpen();
    return this.#stoolap.dbExecBatchBuf(this.#ptr, sql, batchBuf);
  }

  // -- Clone --

  clone() {
    this.#checkOpen();
    const clonedPtr = this.#stoolap.dbClone(this.#ptr);
    return new Database(clonedPtr, this.#stoolap);
  }

  // -- Execute (DML) --

  async execute(sql, params) {
    if (!params) return this._executeAsync(sql);
    if (Array.isArray(params)) {
      if (params.length === 0) return this._executeAsync(sql);
      return this._executeAsync(sql, params);
    }
    const p = processParams(sql, params);
    return p.values
      ? this._executeAsync(p.sql, p.values)
      : this._executeAsync(sql);
  }

  executeSync(sql, params) {
    this.#checkOpen();
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      return this.#stoolap.dbExecNamed(this.#ptr, sql, names, values);
    }
    return this.#stoolap.dbExec(this.#ptr, sql, params);
  }

  async exec(sql) {
    await this._execAsync(sql);
  }

  execSync(sql) {
    this.#checkOpen();
    this.#stoolap.dbExecSimple(this.#ptr, sql);
  }

  // -- Query --

  async query(sql, params) {
    let buf;
    if (!params) {
      buf = await this._queryBufferAsync(sql);
    } else if (Array.isArray(params)) {
      buf =
        params.length === 0
          ? await this._queryBufferAsync(sql)
          : await this._queryBufferAsync(sql, params);
    } else {
      const p = processParams(sql, params);
      buf = p.values
        ? await this._queryBufferAsync(p.sql, p.values)
        : await this._queryBufferAsync(sql);
    }
    return parseRows(buf);
  }

  /* One FFI call. mode: 0 = rows, 1 = first row, 2 = raw buffer */
  #queryAll(sql, params, mode) {
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      return this.#stoolap.dbQueryNamedAll(this.#ptr, null, sql, names, values, mode);
    }
    return this.#stoolap.dbQueryAll(this.#ptr, null, sql, params, mode);
  }

  querySync(sql, params) {
    this.#checkOpen();
    const result = this.#queryAll(sql, params, 0);
    return Array.isArray(result) ? result : parseRows(result);
  }

  async queryOne(sql, params) {
    const rows = await this.query(sql, params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(sql, params) {
    this.#checkOpen();
    const result = this.#queryAll(sql, params, 1);
    if (result instanceof ArrayBuffer) {
      const rows = parseRows(result);
      return rows.length === 0 ? null : rows[0];
    }
    return result;
  }

  async queryRaw(sql, params) {
    let buf;
    if (!params) {
      buf = await this._queryBufferAsync(sql);
    } else if (Array.isArray(params)) {
      buf =
        params.length === 0
          ? await this._queryBufferAsync(sql)
          : await this._queryBufferAsync(sql, params);
    } else {
      const p = processParams(sql, params);
      buf = p.values
        ? await this._queryBufferAsync(p.sql, p.values)
        : await this._queryBufferAsync(sql);
    }
    return parseRaw(buf);
  }

  queryRawSync(sql, params) {
    this.#checkOpen();
    return parseRaw(this.#queryAll(sql, params, 2));
  }

  /**
   * Open a cursor over the query's rows. Iterate it with `for...of` or
   * `for await...of`; rows are fetched in chunks as the loop advances.
   */
  iterate(sql, params) {
    this.#checkOpen();
    let ptr;
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      ptr = this.#stoolap.rowsOpenNamed(this.#ptr, null, sql, names, values);
    } else {
      ptr = this.#stoolap.rowsOpen(this.#ptr, null, sql, params);
    }
    return new RowCursor(this.#stoolap, ptr, this);
  }

  executeBatchSync(sql, paramsArray) {
    this.#checkOpen();
    if (paramsArray.length > 0 && Array.isArray(paramsArray[0])) {
      const batchBuf = encodeBatch(paramsArray);
      return this.#stoolap.dbExecBatchBuf(this.#ptr, sql, batchBuf);
    }

    const tx = this.beginSync();
    try {
      const result = tx.executeBatchSync(sql, paramsArray);
      tx.commitSync();
      return result;
    } catch (e) {
      try {
        tx.rollbackSync();
      } catch {}
      throw e;
    }
  }

  // -- Prepared statements --

  prepare(sql) {
    this.#checkOpen();
    if (typeof sql !== "string") throw new TypeError("sql must be a string");
    const prepared = rewriteNamedParams(sql);
    const [stmtPtr, queryOneIntBound] = this.#stoolap.dbPrepare(
      this.#ptr,
      prepared.sql,
    );
    const stmt = new PreparedStatement(
      stmtPtr,
      this.#stoolap,
      queryOneIntBound,
      this,
      sql,
      prepared.names,
    );
    this._trackChild(stmt);
    return stmt;
  }

  // -- Transactions --

  async begin() {
    const txPtr = await this._beginAsync();
    const tx = new Transaction(txPtr, this.#stoolap, this, this.#ptr);
    this._trackChild(tx);
    return tx;
  }

  beginSync() {
    this.#checkOpen();
    const txPtr = this.#stoolap.txBegin(this.#ptr);
    const tx = new Transaction(txPtr, this.#stoolap, this, this.#ptr);
    this._trackChild(tx);
    return tx;
  }

  // -- Close --

  async close() {
    if (this.#closingPromise) return this.#closingPromise;
    if (!this.#ptr) return;
    const ptr = this.#ptr;
    this.#ptr = null;
    /* let queued work on the handle, its statements and its transactions
     * land before anything is released */
    const settling = [];
    for (const child of this.#children) {
      if (child._settle) settling.push(child._settle());
    }
    const wait = Promise.all(settling).then(() =>
      this.#pendingAsync === 0
        ? undefined
        : new Promise((resolve) => {
            this.#closeWaiter = resolve;
          }),
    );
    this.#closingPromise = wait
      .then(() => {
        this.#closeChildren();
        return this.#stoolap.dbCloseAsync(ptr);
      })
      .finally(() => {
        this.#closingPromise = null;
        this.#closeWaiter = null;
      });
    return this.#closingPromise;
  }

  closeSync() {
    if (this.#closingPromise || this.#pendingAsync > 0 || this.#childrenPending()) {
      throw new Error("Database has pending async operations");
    }
    if (this.#ptr) {
      this.#closeChildren();
      this.#stoolap.dbClose(this.#ptr);
      this.#ptr = null;
    }
  }
}

module.exports = { Database };
