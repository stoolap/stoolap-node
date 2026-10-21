"use strict";

const { processParams, splitNamed } = require("./ffi");
const { parseRows, parseRaw } = require("./parse");
const { RowCursor } = require("./iterator");

class Transaction {
  #ptr = null;
  #stoolap = null;
  #dbPtr = null;
  #pendingAsync = 0;
  #tail = Promise.resolve();
  #closingPromise = null;
  #owner = null;
  #cursors = new Set(); /* WeakRef<RowCursor> */

  constructor(ptr, stoolap, owner, dbPtr) {
    this.#ptr = ptr;
    this.#stoolap = stoolap;
    this.#owner = owner || null;
    this.#dbPtr = dbPtr || null;
  }

  /* Called by the owner when it closes, after async work has settled:
   * an open transaction is rolled back so its handle is released. */
  _invalidate() {
    this.#closeCursors();
    if (this.#ptr) {
      const ptr = this.#ptr;
      this.#ptr = null;
      try {
        this.#stoolap.txRollback(ptr);
      } catch {}
    }
  }

  _settle() {
    return this.#closingPromise || this.#tail;
  }

  _hasPending() {
    return this.#pendingAsync > 0 || this.#closingPromise !== null;
  }

  _trackCursor(ref) {
    this.#cursors.add(ref);
  }

  _untrackCursor(ref) {
    this.#cursors.delete(ref);
  }

  #closeCursors() {
    for (const ref of this.#cursors) {
      const cursor = ref.deref();
      if (cursor) cursor._invalidate();
    }
    this.#cursors.clear();
  }

  _rowsChunk(rowsPtr, maxRows, buf) {
    this.#checkSyncReady();
    return this.#stoolap.rowsChunk(rowsPtr, maxRows, buf);
  }

  _rowsChunkAsync(rowsPtr, maxRows) {
    return this.#enqueueAsync(() => this.#stoolap.rowsChunkAsync(rowsPtr, maxRows));
  }

  /**
   * Open a cursor over the query's rows inside the transaction. Open
   * cursors are closed by commit and rollback.
   */
  iterate(sql, params) {
    this.#checkSyncReady();
    let ptr;
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      ptr = this.#stoolap.rowsOpenNamed(this.#dbPtr, this.#ptr, sql, names, values);
    } else {
      ptr = this.#stoolap.rowsOpen(this.#dbPtr, this.#ptr, sql, params);
    }
    return new RowCursor(this.#stoolap, ptr, this);
  }

  #checkOpen() {
    if (!this.#ptr) throw new Error("Transaction is closed");
  }

  #checkSyncReady() {
    this.#checkOpen();
    if (this.#closingPromise || this.#pendingAsync > 0) {
      throw new Error("Transaction has pending async operations");
    }
  }

  #enqueueAsync(operation) {
    this.#checkOpen();
    if (this.#closingPromise) throw new Error("Transaction is closing");

    const ptr = this.#ptr;
    const run = async () => {
      this.#pendingAsync += 1;
      try {
        return await operation(ptr);
      } finally {
        this.#pendingAsync -= 1;
      }
    };

    const promise = this.#tail.then(run, run);
    this.#tail = promise.catch(() => {});
    return promise;
  }

  // -- Execute --

  async execute(sql, params) {
    return this.#enqueueAsync((ptr) => {
      if (!params) return this.#stoolap.txExecAsync(ptr, sql);
      if (Array.isArray(params)) {
        if (params.length === 0) return this.#stoolap.txExecAsync(ptr, sql);
        return this.#stoolap.txExecAsync(ptr, sql, params);
      }
      const p = processParams(sql, params);
      return p.values
        ? this.#stoolap.txExecAsync(ptr, p.sql, p.values)
        : this.#stoolap.txExecAsync(ptr, sql);
    });
  }

  executeSync(sql, params) {
    this.#checkSyncReady();
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      return this.#stoolap.txExecNamed(this.#ptr, sql, names, values);
    }
    return this.#stoolap.txExec(this.#ptr, sql, params);
  }

  async query(sql, params) {
    const buf = await this.#enqueueAsync((ptr) => {
      if (!params) return this.#stoolap.txQueryBufAsync(ptr, sql);
      if (Array.isArray(params)) {
        if (params.length === 0) return this.#stoolap.txQueryBufAsync(ptr, sql);
        return this.#stoolap.txQueryBufAsync(ptr, sql, params);
      }
      const p = processParams(sql, params);
      return p.values
        ? this.#stoolap.txQueryBufAsync(ptr, p.sql, p.values)
        : this.#stoolap.txQueryBufAsync(ptr, sql);
    });
    return parseRows(buf);
  }

  /* One FFI call inside the transaction. mode: 0 = rows, 1 = first row, 2 = raw buffer */
  #queryAll(sql, params, mode) {
    if (params && !Array.isArray(params)) {
      const [names, values] = splitNamed(params);
      return this.#stoolap.dbQueryNamedAll(this.#dbPtr, this.#ptr, sql, names, values, mode);
    }
    return this.#stoolap.dbQueryAll(this.#dbPtr, this.#ptr, sql, params, mode);
  }

  querySync(sql, params) {
    this.#checkSyncReady();
    const result = this.#queryAll(sql, params, 0);
    return Array.isArray(result) ? result : parseRows(result);
  }

  async queryOne(sql, params) {
    const rows = await this.query(sql, params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(sql, params) {
    this.#checkSyncReady();
    const result = this.#queryAll(sql, params, 1);
    if (result instanceof ArrayBuffer) {
      const rows = parseRows(result);
      return rows.length === 0 ? null : rows[0];
    }
    return result;
  }

  async queryRaw(sql, params) {
    const buf = await this.#enqueueAsync((ptr) => {
      if (!params) return this.#stoolap.txQueryBufAsync(ptr, sql);
      if (Array.isArray(params)) {
        if (params.length === 0) return this.#stoolap.txQueryBufAsync(ptr, sql);
        return this.#stoolap.txQueryBufAsync(ptr, sql, params);
      }
      const p = processParams(sql, params);
      return p.values
        ? this.#stoolap.txQueryBufAsync(ptr, p.sql, p.values)
        : this.#stoolap.txQueryBufAsync(ptr, sql);
    });
    return parseRaw(buf);
  }

  queryRawSync(sql, params) {
    this.#checkSyncReady();
    return parseRaw(this.#queryAll(sql, params, 2));
  }

  executeBatchSync(sql, paramsArray) {
    this.#checkSyncReady();
    if (paramsArray.length > 0 && Array.isArray(paramsArray[0])) {
      return this.#stoolap.txExecBatch(
        this.#dbPtr,
        this.#ptr,
        sql,
        paramsArray,
      );
    }
    let totalChanges = 0;
    for (const params of paramsArray) {
      totalChanges += this.executeSync(sql, params).changes;
    }
    return { changes: totalChanges };
  }

  // -- Commit / Rollback --

  async commit() {
    this.#checkOpen();
    if (this.#closingPromise) return this.#closingPromise;

    const ptr = this.#ptr;
    this.#closingPromise = this.#tail
      .then(() => {
        this.#closeCursors();
        return this.#stoolap.txCommitAsync(ptr);
      })
      .finally(() => {
        this.#ptr = null;
        this.#closingPromise = null;
        if (this.#owner) this.#owner._untrackChild(this);
      });

    this.#tail = this.#closingPromise.catch(() => {});
    return this.#closingPromise;
  }

  commitSync() {
    this.#checkSyncReady();
    this.#closeCursors();
    const ptr = this.#ptr;
    this.#ptr = null;
    if (this.#owner) this.#owner._untrackChild(this);
    this.#stoolap.txCommit(ptr);
  }

  async rollback() {
    this.#checkOpen();
    if (this.#closingPromise) return this.#closingPromise;

    const ptr = this.#ptr;
    this.#closingPromise = this.#tail
      .then(() => {
        this.#closeCursors();
        return this.#stoolap.txRollbackAsync(ptr);
      })
      .finally(() => {
        this.#ptr = null;
        this.#closingPromise = null;
        if (this.#owner) this.#owner._untrackChild(this);
      });

    this.#tail = this.#closingPromise.catch(() => {});
    return this.#closingPromise;
  }

  rollbackSync() {
    this.#checkSyncReady();
    this.#closeCursors();
    const ptr = this.#ptr;
    this.#ptr = null;
    if (this.#owner) this.#owner._untrackChild(this);
    this.#stoolap.txRollback(ptr);
  }
}

module.exports = { Transaction };
