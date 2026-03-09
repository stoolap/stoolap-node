"use strict";

const { processParams } = require("./ffi");
const {
  parseRows,
  parseRaw,
  parseHeader,
  makeRowFactory,
  parseRowsCached,
} = require("./parse");

/* Call stmtExecNums with direct numeric args (no array wrapping).
 * Manual unpack for 1-3 params avoids spread overhead. */
function callExecNums(stoolap, ptr, params) {
  const n = params.length;
  switch (n) {
    case 1:
      return { changes: stoolap.stmtExecNums(ptr, params[0]) };
    case 2:
      return { changes: stoolap.stmtExecNums(ptr, params[0], params[1]) };
    case 3:
      return {
        changes: stoolap.stmtExecNums(ptr, params[0], params[1], params[2]),
      };
    default: {
      const args = [ptr];
      for (let i = 0; i < n; i++) args.push(params[i]);
      return { changes: stoolap.stmtExecNums.apply(stoolap, args) };
    }
  }
}

/* Call stmtQueryNums with direct numeric args. */
function callQueryNums(stoolap, ptr, params) {
  const n = params.length;
  switch (n) {
    case 1:
      return stoolap.stmtQueryNums(ptr, params[0]);
    case 2:
      return stoolap.stmtQueryNums(ptr, params[0], params[1]);
    case 3:
      return stoolap.stmtQueryNums(ptr, params[0], params[1], params[2]);
    default: {
      const args = [ptr];
      for (let i = 0; i < n; i++) args.push(params[i]);
      return stoolap.stmtQueryNums.apply(stoolap, args);
    }
  }
}

/* Call stmtQueryOneNums with direct numeric args. */
function callQueryOneNums(stoolap, ptr, params) {
  const n = params.length;
  switch (n) {
    case 1:
      return stoolap.stmtQueryOneNums(ptr, params[0]);
    case 2:
      return stoolap.stmtQueryOneNums(ptr, params[0], params[1]);
    case 3:
      return stoolap.stmtQueryOneNums(ptr, params[0], params[1], params[2]);
    default: {
      const args = [ptr];
      for (let i = 0; i < n; i++) args.push(params[i]);
      return stoolap.stmtQueryOneNums.apply(stoolap, args);
    }
  }
}

function isAllNums(params) {
  for (let i = 0; i < params.length; i++) {
    if (typeof params[i] !== "number") return false;
  }
  return true;
}

/* ---- Binary param buffer encoding ---- */
/* Tags: 0=NULL, 1=INT32(4B), 2=F64(8B), 3=TEXT(u32+data+\0), 4=BOOL(1B), 5=INT64(8B) */

let _pbuf = Buffer.allocUnsafe(16384);

/* Encode a batch of param arrays into a single buffer.
 * Format: [u32 row_count][u8 params_per_row][row0 params][row1 params]...
 * Returns Buffer subarray ready to pass to C. */
function encodeBatch(paramsArray) {
  const rowCount = paramsArray.length;
  const ppr = paramsArray[0].length;
  let buf = _pbuf;
  const estSize = 5 + rowCount * ppr * 14;
  if (estSize > buf.length) {
    buf = Buffer.allocUnsafe(estSize + 4096);
    _pbuf = buf;
  }
  let off = 0;
  buf.writeUInt32LE(rowCount, off);
  off += 4;
  buf[off++] = ppr;
  for (let r = 0; r < rowCount; r++) {
    const row = paramsArray[r];
    for (let j = 0; j < ppr; j++) {
      const v = row[j];
      if (v === null || v === undefined) {
        buf[off++] = 0;
      } else if (typeof v === "number") {
        const i32 = v | 0;
        if (i32 === v) {
          buf[off++] = 1;
          buf.writeInt32LE(i32, off);
          off += 4;
        } else {
          buf[off++] = 2;
          buf.writeDoubleLE(v, off);
          off += 8;
        }
      } else if (typeof v === "string") {
        const maxBytes = v.length * 3 + 6;
        if (off + maxBytes > buf.length) {
          const nb = Buffer.allocUnsafe(
            Math.max(buf.length * 2, off + maxBytes + 4096),
          );
          buf.copy(nb, 0, 0, off);
          buf = nb;
          _pbuf = nb;
        }
        buf[off++] = 3;
        const written = buf.write(v, off + 4, "utf8");
        buf.writeUInt32LE(written, off);
        off += 4;
        off += written;
        buf[off++] = 0;
      } else if (typeof v === "boolean") {
        buf[off++] = 4;
        buf[off++] = v ? 1 : 0;
      } else if (typeof v === "bigint") {
        buf[off++] = 5;
        buf.writeBigInt64LE(v, off);
        off += 8;
      } else if (typeof v === "object") {
        const s = v instanceof Date ? v.toISOString() : JSON.stringify(v);
        const maxBytes = s.length * 3 + 6;
        if (off + maxBytes > buf.length) {
          const nb = Buffer.allocUnsafe(
            Math.max(buf.length * 2, off + maxBytes + 4096),
          );
          buf.copy(nb, 0, 0, off);
          buf = nb;
          _pbuf = nb;
        }
        buf[off++] = 3;
        const written = buf.write(s, off + 4, "utf8");
        buf.writeUInt32LE(written, off);
        off += 4;
        off += written;
        buf[off++] = 0;
      }
    }
  }
  return buf.subarray(0, off);
}

class PreparedStatement {
  #ptr = null;
  #stoolap = null;
  #q1i = null;
  #factory = null;
  #headerOff = 0;
  #colCount = 0;
  #owner = null;
  #sql = "";
  #preparedSql = "";
  #paramNames = null;

  constructor(
    ptr,
    stoolap,
    queryOneIntBound,
    owner,
    sql,
    preparedSql,
    paramNames,
  ) {
    this.#ptr = ptr;
    this.#stoolap = stoolap;
    this.#q1i = queryOneIntBound || null;
    this.#owner = owner;
    this.#sql = sql;
    this.#preparedSql = preparedSql || sql;
    this.#paramNames = paramNames || null;
  }

  #checkOpen() {
    if (!this.#ptr) throw new Error("PreparedStatement is finalized");
  }

  #parseResult(abuf) {
    if (this.#factory) {
      return parseRowsCached(
        abuf,
        this.#headerOff,
        this.#factory,
        this.#colCount,
      );
    }
    const meta = parseHeader(abuf);
    this.#headerOff = meta.headerOff;
    this.#colCount = meta.colCount;
    const f = makeRowFactory(meta.colNames);
    if (f) {
      this.#factory = f;
      return parseRowsCached(abuf, meta.headerOff, f, meta.colCount);
    }
    return parseRows(abuf);
  }

  #normalizeParams(params) {
    if (!params) {
      return { sql: this.#preparedSql, values: null };
    }
    if (Array.isArray(params)) {
      return {
        sql: this.#preparedSql,
        values: params.length === 0 ? null : params,
      };
    }
    if (this.#paramNames) {
      const values = new Array(this.#paramNames.length);
      for (let i = 0; i < this.#paramNames.length; i++) {
        values[i] = params[this.#paramNames[i]];
      }
      return { sql: this.#preparedSql, values };
    }
    const rewritten = processParams(this.#sql, params);
    return {
      sql: rewritten.sql,
      values: rewritten.values,
    };
  }

  get sql() {
    this.#checkOpen();
    return this.#sql;
  }

  // -- Execute --

  execute(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    return this.#owner._stmtExecAsync(
      this.#ptr,
      normalized.values ? normalized.values : undefined,
    );
  }

  executeSync(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    if (!normalized.values) return this.#stoolap.stmtExec(this.#ptr);
    if (normalized.values.length <= 16 && isAllNums(normalized.values)) {
      return callExecNums(this.#stoolap, this.#ptr, normalized.values);
    }
    return this.#stoolap.stmtExec(this.#ptr, normalized.values);
  }

  // -- Query --

  async query(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    const buf = await this.#owner._stmtQueryBufAsync(
      this.#ptr,
      normalized.values ? normalized.values : undefined,
    );
    return this.#parseResult(buf);
  }

  querySync(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    if (!normalized.values) {
      const result = this.#stoolap.stmtQuery(this.#ptr);
      return Array.isArray(result) ? result : this.#parseResult(result);
    }
    if (normalized.values.length <= 16 && isAllNums(normalized.values)) {
      const result = callQueryNums(this.#stoolap, this.#ptr, normalized.values);
      return Array.isArray(result) ? result : this.#parseResult(result);
    }
    const result = this.#stoolap.stmtQuery(this.#ptr, normalized.values);
    return Array.isArray(result) ? result : this.#parseResult(result);
  }

  // -- QueryOne --

  async queryOne(params) {
    const rows = await this.query(params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    if (!normalized.values) return this.#stoolap.stmtQueryOne(this.#ptr);
    if (
      normalized.values.length === 1 &&
      typeof normalized.values[0] === "number"
    ) {
      return this.#q1i(normalized.values[0]);
    }
    if (normalized.values.length <= 16 && isAllNums(normalized.values)) {
      return callQueryOneNums(this.#stoolap, this.#ptr, normalized.values);
    }
    return this.#stoolap.stmtQueryOne(this.#ptr, normalized.values);
  }

  // -- QueryRaw --

  async queryRaw(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    const buf = await this.#owner._stmtQueryBufAsync(
      this.#ptr,
      normalized.values ? normalized.values : undefined,
    );
    return parseRaw(buf);
  }

  queryRawSync(params) {
    this.#checkOpen();
    const normalized = this.#normalizeParams(params);
    if (!normalized.values) {
      return parseRaw(this.#stoolap.stmtQueryBuf(this.#ptr));
    }
    return parseRaw(this.#stoolap.stmtQueryBuf(this.#ptr, normalized.values));
  }

  // -- Batch --

  executeBatchSync(paramsArray) {
    this.#checkOpen();
    if (paramsArray.length === 0) return { changes: 0 };
    if (paramsArray.length > 0 && Array.isArray(paramsArray[0])) {
      const batchBuf = encodeBatch(paramsArray);
      return this.#stoolap.stmtExecBatchBuf(this.#ptr, batchBuf);
    }
    let totalChanges = 0;
    for (const params of paramsArray) {
      const normalized = this.#normalizeParams(params);
      const result = normalized.values
        ? this.#stoolap.stmtExec(this.#ptr, normalized.values)
        : this.#stoolap.stmtExec(this.#ptr);
      totalChanges += result.changes;
    }
    return { changes: totalChanges };
  }

  _invalidate() {
    if (this.#ptr) {
      this.#stoolap.stmtFinalize(this.#ptr);
      this.#ptr = null;
    }
  }

  finalize() {
    if (this.#ptr) {
      this.#stoolap.stmtFinalize(this.#ptr);
      this.#ptr = null;
      if (this.#owner) this.#owner._untrackChild(this);
    }
  }
}

module.exports = { PreparedStatement, encodeBatch };
