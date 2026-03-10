"use strict";

const { loadStoolap, processParams, rewriteNamedParams } = require("./ffi");
const { parseRows, parseRaw } = require("./parse");
const { Transaction } = require("./transaction");
const { PreparedStatement, encodeBatch } = require("./statement");

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

  #closeChildren() {
    for (const child of this.#children) {
      child._invalidate();
    }
    this.#children.clear();
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

  _executeBatchBufferSync(sql, batchBuf) {
    this.#checkOpen();
    return this.#stoolap.dbExecBatchBuf(this.#ptr, sql, batchBuf);
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
    if (!params) return this.#stoolap.dbExec(this.#ptr, sql);
    if (Array.isArray(params)) {
      if (params.length === 0) return this.#stoolap.dbExec(this.#ptr, sql);
      return this.#stoolap.dbExec(this.#ptr, sql, params);
    }
    const p = processParams(sql, params);
    return p.values
      ? this.#stoolap.dbExec(this.#ptr, p.sql, p.values)
      : this.#stoolap.dbExec(this.#ptr, sql);
  }

  // -- Exec (DDL) --

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

  querySync(sql, params) {
    this.#checkOpen();
    let result;
    if (!params) {
      result = this.#stoolap.dbQuery(this.#ptr, sql);
    } else if (Array.isArray(params)) {
      result =
        params.length === 0
          ? this.#stoolap.dbQuery(this.#ptr, sql)
          : this.#stoolap.dbQuery(this.#ptr, sql, params);
    } else {
      const p = processParams(sql, params);
      result = p.values
        ? this.#stoolap.dbQuery(this.#ptr, p.sql, p.values)
        : this.#stoolap.dbQuery(this.#ptr, sql);
    }
    return Array.isArray(result) ? result : parseRows(result);
  }

  // -- QueryOne --

  async queryOne(sql, params) {
    const rows = await this.query(sql, params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(sql, params) {
    this.#checkOpen();
    if (!params) return this.#stoolap.dbQueryOne(this.#ptr, sql);
    if (Array.isArray(params)) {
      if (params.length === 0) return this.#stoolap.dbQueryOne(this.#ptr, sql);
      return this.#stoolap.dbQueryOne(this.#ptr, sql, params);
    }
    const p = processParams(sql, params);
    return p.values
      ? this.#stoolap.dbQueryOne(this.#ptr, p.sql, p.values)
      : this.#stoolap.dbQueryOne(this.#ptr, sql);
  }

  // -- QueryRaw --

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
    let buf;
    if (!params) {
      buf = this.#stoolap.dbQueryBuf(this.#ptr, sql);
    } else if (Array.isArray(params)) {
      buf =
        params.length === 0
          ? this.#stoolap.dbQueryBuf(this.#ptr, sql)
          : this.#stoolap.dbQueryBuf(this.#ptr, sql, params);
    } else {
      const p = processParams(sql, params);
      buf = p.values
        ? this.#stoolap.dbQueryBuf(this.#ptr, p.sql, p.values)
        : this.#stoolap.dbQueryBuf(this.#ptr, sql);
    }
    return parseRaw(buf);
  }

  // -- Batch --

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
      prepared.sql,
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

    this.#closeChildren();
    const ptr = this.#ptr;
    this.#ptr = null;

    const wait =
      this.#pendingAsync === 0
        ? Promise.resolve()
        : new Promise((resolve) => {
            this.#closeWaiter = resolve;
          });

    this.#closingPromise = wait
      .then(() => this.#stoolap.dbCloseAsync(ptr))
      .finally(() => {
        this.#closingPromise = null;
        this.#closeWaiter = null;
      });

    return this.#closingPromise;
  }

  closeSync() {
    if (this.#closingPromise || this.#pendingAsync > 0) {
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
