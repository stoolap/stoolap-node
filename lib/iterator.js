"use strict";

const {
  parseRows,
  parseHeader,
  makeRowFactory,
  parseRowsCached,
  parseRawRows,
} = require("./parse");

const FIRST_CHUNK = 32;
const MAX_CHUNK = 256;

/**
 * A row cursor over an open result set. Rows cross the FFI boundary in
 * packed chunks (32 rows, doubling to 256), so a scan costs one call per
 * chunk instead of one per row. Iterate it synchronously or with
 * `for await`; the cursor closes itself when iteration ends, breaks or
 * throws, and `close()` ends it early. A raw cursor yields each row as an
 * array and keeps duplicate column names apart; `columns` names them.
 */
class RowCursor {
  #stoolap = null;
  #ptr = null;
  #owner = null;
  #raw = false;
  #chunk = FIRST_CHUNK;
  #buf = null;
  #meta = null;
  #factory = null;
  #ahead = null; /* rows fetched by `columns` before the loop started */
  #pending = false;
  #closeWanted = false;
  #ref = null;

  constructor(stoolap, ptr, owner, raw) {
    this.#stoolap = stoolap;
    this.#ptr = ptr;
    this.#owner = owner;
    this.#raw = raw === true;
    /* the owner holds only a weak reference: an abandoned cursor can be
     * collected, and the native handle's finalizer closes it */
    this.#ref = new WeakRef(this);
    owner._trackCursor(this.#ref);
  }

  #release() {
    if (this.#ptr) {
      this.#stoolap.rowsClose(this.#ptr);
      this.#ptr = null;
    }
    this.#closeWanted = false;
    this.#owner._untrackCursor(this.#ref);
  }

  /** Called by the owner when it closes. A chunk in flight closes the cursor when it lands. */
  _invalidate() {
    if (this.#pending) this.#closeWanted = true;
    else this.#release();
  }

  close() {
    if (this.#pending) this.#closeWanted = true;
    else this.#release();
  }

  get closed() {
    return this.#ptr === null;
  }

  /**
   * The column names, in result order. Known once the first chunk has
   * landed; read before the loop, it fetches that chunk synchronously.
   */
  get columns() {
    if (!this.#meta && this.#ptr) {
      if (this.#pending) throw new Error("A chunk fetch is in flight");
      this.#ahead = this.#fetchSync();
    }
    return this.#meta ? this.#meta.colNames : [];
  }

  /* Parse one chunk. A chunk with no rows ends the result; its header
   * still names the columns of an empty result. */
  #parse(buf) {
    if (!this.#meta) {
      this.#meta = parseHeader(buf);
      if (!this.#raw) this.#factory = makeRowFactory(this.#meta.colNames);
    }
    let rows;
    if (this.#raw) {
      rows = parseRawRows(buf, this.#meta.headerOff, this.#meta.colCount);
    } else if (this.#factory) {
      rows = parseRowsCached(buf, this.#meta.headerOff, this.#factory, this.#meta.colCount);
    } else {
      rows = parseRows(buf);
    }
    if (this.#chunk < MAX_CHUNK) this.#chunk *= 2;
    return rows;
  }

  #fetchSync() {
    const buf = this.#owner._rowsChunk(this.#ptr, this.#chunk, this.#buf);
    if (!buf) return null;
    this.#buf = buf;
    return this.#parse(buf);
  }

  #takeAhead() {
    const rows = this.#ahead;
    this.#ahead = null;
    return rows;
  }

  *[Symbol.iterator]() {
    try {
      while (this.#ptr) {
        const rows = this.#takeAhead() || this.#fetchSync();
        if (!rows || rows.length === 0) break;
        for (let i = 0; i < rows.length; i++) yield rows[i];
      }
    } finally {
      this.close();
    }
  }

  async *[Symbol.asyncIterator]() {
    try {
      while (this.#ptr) {
        let rows = this.#takeAhead();
        if (!rows) {
          this.#pending = true;
          let buf;
          try {
            buf = await this.#owner._rowsChunkAsync(this.#ptr, this.#chunk);
          } finally {
            this.#pending = false;
          }
          if (this.#closeWanted) {
            this.#release();
            break;
          }
          if (!buf) break;
          rows = this.#parse(buf);
        }
        if (rows.length === 0) break;
        for (let i = 0; i < rows.length; i++) yield rows[i];
      }
    } finally {
      this.close();
    }
  }
}

module.exports = { RowCursor };
