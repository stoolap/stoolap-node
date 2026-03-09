"use strict";

const { processParams } = require("./ffi");
const { parseRows, parseRaw } = require("./parse");

class Transaction {
  #ptr = null;
  #stoolap = null;
  #pendingAsync = 0;
  #tail = Promise.resolve();
  #closingPromise = null;
  #owner = null;

  constructor(ptr, stoolap, owner) {
    this.#ptr = ptr;
    this.#stoolap = stoolap;
    this.#owner = owner || null;
  }

  _invalidate() {
    this.#ptr = null;
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
    if (!params) return this.#stoolap.txExec(this.#ptr, sql);
    if (Array.isArray(params)) {
      if (params.length === 0) return this.#stoolap.txExec(this.#ptr, sql);
      return this.#stoolap.txExec(this.#ptr, sql, params);
    }
    const p = processParams(sql, params);
    return p.values
      ? this.#stoolap.txExec(this.#ptr, p.sql, p.values)
      : this.#stoolap.txExec(this.#ptr, sql);
  }

  // -- Query --

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

  querySync(sql, params) {
    this.#checkSyncReady();
    let result;
    if (!params) {
      result = this.#stoolap.txQuery(this.#ptr, sql);
    } else if (Array.isArray(params)) {
      result =
        params.length === 0
          ? this.#stoolap.txQuery(this.#ptr, sql)
          : this.#stoolap.txQuery(this.#ptr, sql, params);
    } else {
      const p = processParams(sql, params);
      result = p.values
        ? this.#stoolap.txQuery(this.#ptr, p.sql, p.values)
        : this.#stoolap.txQuery(this.#ptr, sql);
    }
    return Array.isArray(result) ? result : parseRows(result);
  }

  // -- QueryOne --

  async queryOne(sql, params) {
    const rows = await this.query(sql, params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(sql, params) {
    this.#checkSyncReady();
    if (!params) return this.#stoolap.txQueryOne(this.#ptr, sql);
    if (Array.isArray(params)) {
      if (params.length === 0) return this.#stoolap.txQueryOne(this.#ptr, sql);
      return this.#stoolap.txQueryOne(this.#ptr, sql, params);
    }
    const p = processParams(sql, params);
    return p.values
      ? this.#stoolap.txQueryOne(this.#ptr, p.sql, p.values)
      : this.#stoolap.txQueryOne(this.#ptr, sql);
  }

  // -- QueryRaw --

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
    let buf;
    if (!params) {
      buf = this.#stoolap.txQueryBuf(this.#ptr, sql);
    } else if (Array.isArray(params)) {
      buf =
        params.length === 0
          ? this.#stoolap.txQueryBuf(this.#ptr, sql)
          : this.#stoolap.txQueryBuf(this.#ptr, sql, params);
    } else {
      const p = processParams(sql, params);
      buf = p.values
        ? this.#stoolap.txQueryBuf(this.#ptr, p.sql, p.values)
        : this.#stoolap.txQueryBuf(this.#ptr, sql);
    }
    return parseRaw(buf);
  }

  // -- Batch --

  executeBatchSync(sql, paramsArray) {
    this.#checkSyncReady();
    if (paramsArray.length > 0 && Array.isArray(paramsArray[0])) {
      return this.#stoolap.txExecBatch(this.#ptr, sql, paramsArray);
    }
    let totalChanges = 0;
    for (const params of paramsArray) {
      const p = processParams(sql, params);
      const result = p.values
        ? this.#stoolap.txExec(this.#ptr, p.sql, p.values)
        : this.#stoolap.txExec(this.#ptr, sql);
      totalChanges += result.changes;
    }
    return { changes: totalChanges };
  }

  // -- Commit / Rollback --

  async commit() {
    this.#checkOpen();
    if (this.#closingPromise) return this.#closingPromise;

    const ptr = this.#ptr;
    this.#closingPromise = this.#tail
      .then(() => this.#stoolap.txCommitAsync(ptr))
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
    const ptr = this.#ptr;
    this.#ptr = null;
    this.#stoolap.txCommit(ptr);
    if (this.#owner) this.#owner._untrackChild(this);
  }

  async rollback() {
    this.#checkOpen();
    if (this.#closingPromise) return this.#closingPromise;

    const ptr = this.#ptr;
    this.#closingPromise = this.#tail
      .then(() => this.#stoolap.txRollbackAsync(ptr))
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
    const ptr = this.#ptr;
    this.#ptr = null;
    this.#stoolap.txRollback(ptr);
    if (this.#owner) this.#owner._untrackChild(this);
  }
}

module.exports = { Transaction };
