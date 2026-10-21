"use strict";

const {
  parseRows,
  parseRaw,
  parseHeader,
  makeRowFactory,
  parseRowsCached,
} = require("./parse");
const { RowCursor } = require("./iterator");

function sameHeader(bytes, header) {
  if (!header || bytes.length < header.length) return false;
  for (let i = 0; i < header.length; i++) {
    if (bytes[i] !== header[i]) return false;
  }
  return true;
}

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
  if (ppr > 65535) throw new RangeError("Too many parameters per row");
  let buf = _pbuf;
  const estSize = 6 + rowCount * ppr * 14;
  if (estSize > buf.length) {
    buf = Buffer.allocUnsafe(estSize + 4096);
    _pbuf = buf;
  }
  let off = 0;
  buf.writeUInt32LE(rowCount, off);
  off += 4;
  buf.writeUInt16LE(ppr, off);
  off += 2;
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
      } else if (v instanceof Date) {
        const ms = v.getTime();
        if (ms !== ms) {
          buf[off++] = 0;
        } else {
          buf[off++] = 6;
          buf.writeBigInt64LE(BigInt(Math.trunc(ms)) * 1000000n, off);
          off += 8;
        }
      } else if (v instanceof Float32Array || v instanceof Uint8Array) {
        /* Float32Array binds as a vector; Buffer and other Uint8Array as text bytes */
        const bytes =
          v instanceof Float32Array
            ? new Uint8Array(v.buffer, v.byteOffset, v.byteLength)
            : v;
        const need = bytes.length + 6;
        if (off + need > buf.length) {
          const nb = Buffer.allocUnsafe(Math.max(buf.length * 2, off + need + 4096));
          buf.copy(nb, 0, 0, off);
          buf = nb;
          _pbuf = nb;
        }
        if (v instanceof Float32Array) {
          buf[off++] = 7;
          buf.writeUInt32LE(bytes.length, off);
          off += 4;
          buf.set(bytes, off);
          off += bytes.length;
        } else {
          buf[off++] = 3;
          buf.writeUInt32LE(bytes.length, off);
          off += 4;
          buf.set(bytes, off);
          off += bytes.length;
          buf[off++] = 0;
        }
      } else if (typeof v === "object") {
        const s = JSON.stringify(v);
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
  #ptr = null; /* null once finalized or finalize is pending */
  #native = null; /* the handle, until it is actually released */
  #stoolap = null;
  #q1i = null;
  #factory = null;
  #headerOff = 0;
  #colCount = 0;
  #owner = null;
  #sql = "";
  #paramNames = null;
  #header = null;
  #pending = 0;
  #finalizeWanted = false;

  constructor(ptr, stoolap, queryOneIntBound, owner, sql, paramNames) {
    this.#ptr = ptr;
    this.#native = ptr;
    this.#stoolap = stoolap;
    this.#q1i = queryOneIntBound || null;
    this.#owner = owner;
    this.#sql = sql;
    this.#paramNames = paramNames || null;
  }

  #checkOpen() {
    if (!this.#ptr) throw new Error("PreparedStatement is finalized");
  }

  /* Parse a result buffer with the row factory cached from the first result,
   * as long as the packed header still matches (a schema change after
   * prepare gives the statement new columns). */
  #parseResult(abuf) {
    const bytes = new Uint8Array(abuf);
    if (this.#factory && sameHeader(bytes, this.#header)) {
      return parseRowsCached(abuf, this.#headerOff, this.#factory, this.#colCount);
    }
    const meta = parseHeader(abuf);
    this.#headerOff = meta.headerOff;
    this.#colCount = meta.colCount;
    this.#header = bytes.slice(0, meta.headerOff);
    this.#factory = makeRowFactory(meta.colNames);
    return this.#factory
      ? parseRowsCached(abuf, meta.headerOff, this.#factory, meta.colCount)
      : parseRows(abuf);
  }

  /* Run an async native call; finalize() waits for the last one to land. */
  async #inflight(promise) {
    this.#pending += 1;
    try {
      return await promise;
    } finally {
      this.#pending -= 1;
      if (this.#finalizeWanted && this.#pending === 0) this.#release();
    }
  }

  _hasPending() {
    return this.#pending > 0;
  }

  /* Bind params positionally: an object's values are taken in the order of
   * the :name placeholders the statement was prepared with. */
  #positional(params) {
    if (!params) return null;
    if (Array.isArray(params)) return params.length === 0 ? null : params;
    const names = this.#paramNames;
    if (!names) return null;
    const values = new Array(names.length);
    for (let i = 0; i < names.length; i++) values[i] = params[names[i]];
    return values;
  }

  get sql() {
    this.#checkOpen();
    return this.#sql;
  }

  // -- Execute --

  execute(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    return this.#inflight(this.#owner._stmtExecAsync(this.#ptr, values || undefined));
  }

  executeSync(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    if (!values) return this.#stoolap.stmtExec(this.#ptr);
    if (values.length <= 16 && isAllNums(values)) {
      return callExecNums(this.#stoolap, this.#ptr, values);
    }
    return this.#stoolap.stmtExec(this.#ptr, values);
  }

  // -- Query --

  async query(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    const buf = await this.#inflight(
      this.#owner._stmtQueryBufAsync(this.#ptr, values || undefined),
    );
    return this.#parseResult(buf);
  }

  querySync(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    const result =
      values && values.length <= 16 && isAllNums(values)
        ? callQueryNums(this.#stoolap, this.#ptr, values)
        : this.#stoolap.stmtQueryAll(this.#ptr, values, 0);
    return Array.isArray(result) ? result : this.#parseResult(result);
  }

  // -- QueryOne --

  async queryOne(params) {
    const rows = await this.query(params);
    return rows.length === 0 ? null : rows[0];
  }

  queryOneSync(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    let result;
    if (!values) {
      result = this.#stoolap.stmtQueryAll(this.#ptr, undefined, 1);
    } else if (values.length === 1 && typeof values[0] === "number") {
      result = this.#q1i(values[0]);
    } else if (values.length <= 16 && isAllNums(values)) {
      result = callQueryOneNums(this.#stoolap, this.#ptr, values);
    } else {
      result = this.#stoolap.stmtQueryAll(this.#ptr, values, 1);
    }
    if (result instanceof ArrayBuffer) {
      const rows = this.#parseResult(result);
      return rows.length === 0 ? null : rows[0];
    }
    return result;
  }

  // -- QueryRaw --

  async queryRaw(params) {
    this.#checkOpen();
    const values = this.#positional(params);
    const buf = await this.#inflight(
      this.#owner._stmtQueryBufAsync(this.#ptr, values || undefined),
    );
    return parseRaw(buf);
  }

  queryRawSync(params) {
    this.#checkOpen();
    return parseRaw(this.#stoolap.stmtQueryAll(this.#ptr, this.#positional(params), 2));
  }

  // -- Iterate --

  /** Open a cursor over the statement's rows; iterate with `for...of` or `for await...of`. */
  iterate(params) {
    this.#checkOpen();
    const ptr = this.#stoolap.rowsOpenStmt(this.#ptr, this.#positional(params));
    return new RowCursor(this.#stoolap, ptr, this.#owner);
  }

  // -- Batch --

  executeBatchSync(paramsArray) {
    this.#checkOpen();
    if (paramsArray.length === 0) return { changes: 0 };
    if (Array.isArray(paramsArray[0])) {
      const batchBuf = encodeBatch(paramsArray);
      return this.#stoolap.stmtExecBatchBuf(this.#ptr, batchBuf);
    }
    let totalChanges = 0;
    for (const params of paramsArray) {
      totalChanges += this.executeSync(params).changes;
    }
    return { changes: totalChanges };
  }

  #release() {
    if (this.#native) {
      this.#stoolap.stmtFinalize(this.#native);
      this.#native = null;
    }
    this.#ptr = null;
    this.#finalizeWanted = false;
    if (this.#owner) this.#owner._untrackChild(this);
  }

  /* Called by the owner when it closes; it has already waited for async work. */
  _invalidate() {
    if (this.#pending > 0) this.#finalizeWanted = true;
    else this.#release();
  }

  /** Release the statement. Async calls still in flight finish first. */
  finalize() {
    if (this.#pending > 0) {
      this.#finalizeWanted = true;
      this.#ptr = null; /* no new calls; the handle is freed when the last one lands */
      return;
    }
    this.#release();
  }
}

module.exports = { PreparedStatement, encodeBatch };
