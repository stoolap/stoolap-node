"use strict";

// Optimized pure-JS buffer parser for stoolap_rows_fetch_all binary format.
// Works on Node, Bun, Deno. No native dependencies.
//
// Key optimizations vs naive approach:
//   - Module-level _off eliminates [value, offset] tuple allocations (~70K/query)
//   - Single Buffer.from(abuf) per query on Node for fast string decoding
//   - Buffer.toString('utf8', start, end) avoids per-cell Uint8Array views

const _hasBuffer = typeof Buffer !== "undefined";
const td = new TextDecoder();

let _off = 0;

function readVal(bytes, dv, nb) {
  const tag = bytes[_off++];
  switch (tag) {
    case 0:
      return null;
    case 1: {
      // INTEGER: read as two u32 to avoid BigInt
      const lo = dv.getUint32(_off, true);
      const hi = dv.getInt32(_off + 4, true);
      _off += 8;
      return hi * 0x100000000 + lo;
    }
    case 2: {
      // FLOAT
      const v = dv.getFloat64(_off, true);
      _off += 8;
      return v;
    }
    case 3: // TEXT
    case 6: {
      // JSON
      const len = dv.getUint32(_off, true);
      _off += 4;
      const start = _off;
      _off += len;
      return nb
        ? nb.toString("utf8", start, start + len)
        : td.decode(
            new Uint8Array(bytes.buffer, bytes.byteOffset + start, len),
          );
    }
    case 4: // BOOLEAN
      return bytes[_off++] !== 0;
    case 5: {
      // TIMESTAMP
      const lo = dv.getUint32(_off, true);
      const hi = dv.getInt32(_off + 4, true);
      _off += 8;
      return new Date((hi * 0x100000000 + lo) / 1_000_000);
    }
    case 7: {
      // BLOB (vector)
      const len = dv.getUint32(_off, true);
      _off += 4;
      if (len === 0) return new Float32Array(0);
      const ab = new ArrayBuffer(len);
      new Uint8Array(ab).set(bytes.subarray(_off, _off + len));
      _off += len;
      return new Float32Array(ab);
    }
    default:
      return null;
  }
}

function parseRows(abuf) {
  if (!abuf) return [];
  const bytes = new Uint8Array(abuf);
  const dv = new DataView(abuf);
  const nb = _hasBuffer ? Buffer.from(abuf) : null;

  _off = 0;
  const colCount = dv.getUint32(0, true);
  _off = 4;
  const colNames = new Array(colCount);
  for (let i = 0; i < colCount; i++) {
    const len = dv.getUint16(_off, true);
    _off += 2;
    colNames[i] = nb
      ? nb.toString("utf8", _off, _off + len)
      : td.decode(new Uint8Array(abuf, _off, len));
    _off += len;
  }
  const rowCount = dv.getUint32(_off, true);
  _off += 4;

  const results = new Array(rowCount);
  for (let r = 0; r < rowCount; r++) {
    const row = {};
    for (let c = 0; c < colCount; c++) {
      row[colNames[c]] = readVal(bytes, dv, nb);
    }
    results[r] = row;
  }
  return results;
}

function parseRaw(abuf) {
  if (!abuf) return { columns: [], rows: [] };
  const bytes = new Uint8Array(abuf);
  const dv = new DataView(abuf);
  const nb = _hasBuffer ? Buffer.from(abuf) : null;

  _off = 0;
  const colCount = dv.getUint32(0, true);
  _off = 4;
  const colNames = new Array(colCount);
  for (let i = 0; i < colCount; i++) {
    const len = dv.getUint16(_off, true);
    _off += 2;
    colNames[i] = nb
      ? nb.toString("utf8", _off, _off + len)
      : td.decode(new Uint8Array(abuf, _off, len));
    _off += len;
  }
  const rowCount = dv.getUint32(_off, true);
  _off += 4;

  const rows = new Array(rowCount);
  for (let r = 0; r < rowCount; r++) {
    const row = new Array(colCount);
    for (let c = 0; c < colCount; c++) {
      row[c] = readVal(bytes, dv, nb);
    }
    rows[r] = row;
  }
  return { columns: colNames, rows };
}

/* ---- Cached prepared-statement fast path ---- */

/* Parse just the header (column names) and return metadata for caching. */
function parseHeader(abuf) {
  const dv = new DataView(abuf);
  const nb = _hasBuffer ? Buffer.from(abuf) : null;
  let off = 0;
  const colCount = dv.getUint32(0, true);
  off = 4;
  const colNames = new Array(colCount);
  for (let i = 0; i < colCount; i++) {
    const len = dv.getUint16(off, true);
    off += 2;
    colNames[i] = nb
      ? nb.toString("utf8", off, off + len)
      : td.decode(new Uint8Array(abuf, off, len));
    off += len;
  }
  return { colNames, headerOff: off, colCount };
}

/* Build a compiled row-constructor for fast hidden class creation. */
function makeRowFactory(colNames) {
  const n = colNames.length;
  if (n === 0 || n > 16) return null;
  try {
    const params = new Array(n);
    const props = new Array(n);
    for (let i = 0; i < n; i++) {
      params[i] = "v" + i;
      props[i] = JSON.stringify(colNames[i]) + ":v" + i;
    }
    return new Function(params.join(","), "return {" + props.join(",") + "}");
  } catch {
    return null;
  }
}

/* Parse rows using cached column info + row factory. Skips header re-parsing. */
function parseRowsCached(abuf, headerOff, factory, colCount) {
  const bytes = new Uint8Array(abuf);
  const dv = new DataView(abuf);
  const nb = _hasBuffer ? Buffer.from(abuf) : null;
  _off = headerOff;
  const rowCount = dv.getUint32(_off, true);
  _off += 4;
  const results = new Array(rowCount);
  switch (colCount) {
    case 1:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(readVal(bytes, dv, nb));
      break;
    case 2:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(readVal(bytes, dv, nb), readVal(bytes, dv, nb));
      break;
    case 3:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
        );
      break;
    case 4:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
        );
      break;
    case 5:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
        );
      break;
    case 6:
      for (let r = 0; r < rowCount; r++)
        results[r] = factory(
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
          readVal(bytes, dv, nb),
        );
      break;
    default: {
      const v = new Array(colCount);
      for (let r = 0; r < rowCount; r++) {
        for (let c = 0; c < colCount; c++) v[c] = readVal(bytes, dv, nb);
        results[r] = factory.apply(null, v);
      }
    }
  }
  return results;
}

module.exports = {
  parseRows,
  parseRaw,
  parseHeader,
  makeRowFactory,
  parseRowsCached,
};
