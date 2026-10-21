"use strict";

const { parseRows, parseHeader, makeRowFactory, parseRowsCached } = require("./parse");

const FIRST_CHUNK = 32;
const MAX_CHUNK = 256;

/**
 * A row cursor over an open result set. Rows cross the FFI boundary in
 * packed chunks (32 rows, doubling to 256), so a scan costs one call per
 * chunk instead of one per row. Iterate it synchronously or with
 * `for await`; the cursor closes itself when iteration ends, breaks or
 * throws, and `close()` ends it early.
 */
class RowCursor {
  #stoolap = null;
  #ptr = null;
  #owner = null;
  #chunk = FIRST_CHUNK;
  #buf = null;
  #meta = null;
  #factory = null;
  #pending = false;
  #closeWanted = false;
  #ref = null;

  constructor(stoolap, ptr, owner) {
    this.#stoolap = stoolap;
    this.#ptr = ptr;
    this.#owner = owner;
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

  #parse(buf) {
    if (!this.#meta) {
      this.#meta = parseHeader(buf);
      this.#factory = makeRowFactory(this.#meta.colNames);
    }
    const rows = this.#factory
      ? parseRowsCached(buf, this.#meta.headerOff, this.#factory, this.#meta.colCount)
      : parseRows(buf);
    if (this.#chunk < MAX_CHUNK) this.#chunk *= 2;
    return rows;
  }

  *[Symbol.iterator]() {
    try {
      while (this.#ptr) {
        const buf = this.#owner._rowsChunk(this.#ptr, this.#chunk, this.#buf);
        if (!buf) break;
        this.#buf = buf;
        const rows = this.#parse(buf);
        for (let i = 0; i < rows.length; i++) yield rows[i];
      }
    } finally {
      this.close();
    }
  }

  async *[Symbol.asyncIterator]() {
    try {
      while (this.#ptr) {
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
        const rows = this.#parse(buf);
        for (let i = 0; i < rows.length; i++) yield rows[i];
      }
    } finally {
      this.close();
    }
  }
}

module.exports = { RowCursor };
