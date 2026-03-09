"use strict";

const path = require("node:path");

// -- Library path resolution --
function findLibraryPath() {
  if (process.env.STOOLAP_LIB_PATH) {
    return process.env.STOOLAP_LIB_PATH;
  }

  const platform = process.platform;
  const arch = process.arch;
  const ext =
    platform === "win32" ? "dll" : platform === "darwin" ? "dylib" : "so";
  const prefix = platform === "win32" ? "" : "lib";
  const libName = `${prefix}stoolap.${ext}`;

  const platformMap = {
    "darwin-arm64": "@stoolap/lib-darwin-arm64",
    "darwin-x64": "@stoolap/lib-darwin-x64",
    "linux-arm64": "@stoolap/lib-linux-arm64-gnu",
    "linux-x64": "@stoolap/lib-linux-x64-gnu",
    "win32-x64": "@stoolap/lib-win32-x64-msvc",
  };

  const key = `${platform}-${arch}`;
  const pkg = platformMap[key];
  if (!pkg) {
    throw new Error(`Unsupported platform: ${platform}/${arch}`);
  }

  try {
    const pkgDir = path.dirname(require.resolve(`${pkg}/package.json`));
    return path.join(pkgDir, libName);
  } catch {
    const localPath = path.join(__dirname, "..", libName);
    try {
      require("node:fs").accessSync(localPath);
      return localPath;
    } catch {
      throw new Error(
        `Could not find stoolap shared library. Install the platform package: npm install ${pkg}`,
      );
    }
  }
}

// -- Load native stoolap module --
let _stoolap = null;

function loadStoolap() {
  if (_stoolap) return _stoolap;

  _stoolap = require("../build/Release/stoolap.node");
  _stoolap.loadLibrary(findLibraryPath());
  return _stoolap;
}

// -- Named parameter rewriting --
function isIdentStart(ch) {
  return ch !== undefined && /[A-Za-z_]/.test(ch);
}

function isIdentPart(ch) {
  return ch !== undefined && /[A-Za-z0-9_]/.test(ch);
}

function scanSql(sql, onParam) {
  const out = [];

  for (let i = 0; i < sql.length; ) {
    const ch = sql[i];

    if (ch === "'") {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'" && sql[j + 1] === "'") {
          j += 2;
          continue;
        }
        if (sql[j] === "'") {
          j += 1;
          break;
        }
        j += 1;
      }
      out.push(sql.slice(i, j));
      i = j;
      continue;
    }

    if (ch === '"') {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === '"' && sql[j + 1] === '"') {
          j += 2;
          continue;
        }
        if (sql[j] === '"') {
          j += 1;
          break;
        }
        j += 1;
      }
      out.push(sql.slice(i, j));
      i = j;
      continue;
    }

    if (ch === "-" && sql[i + 1] === "-") {
      let j = i + 2;
      while (j < sql.length && sql[j] !== "\n") j += 1;
      out.push(sql.slice(i, j));
      i = j;
      continue;
    }

    if (ch === "/" && sql[i + 1] === "*") {
      let j = i + 2;
      while (j < sql.length && !(sql[j] === "*" && sql[j + 1] === "/")) j += 1;
      j = j < sql.length ? j + 2 : j;
      out.push(sql.slice(i, j));
      i = j;
      continue;
    }

    if (ch === ":" && sql[i + 1] !== ":" && isIdentStart(sql[i + 1])) {
      let j = i + 2;
      while (j < sql.length && isIdentPart(sql[j])) j += 1;
      out.push(onParam(sql.slice(i + 1, j)));
      i = j;
      continue;
    }

    out.push(ch);
    i += 1;
  }

  return out.join("");
}

function processParams(sql, params) {
  if (!params) return { sql, values: null };

  if (Array.isArray(params)) {
    if (params.length === 0) return { sql, values: null };
    return { sql, values: params };
  }

  const seen = new Map();
  const values = [];
  const rewritten = scanSql(sql, (name) => {
    if (!seen.has(name)) {
      seen.set(name, values.length + 1);
      values.push(params[name]);
    }
    return `$${seen.get(name)}`;
  });

  if (values.length === 0) return { sql, values: null };
  return { sql: rewritten, values };
}

function rewriteNamedParams(sql) {
  const seen = new Map();
  const names = [];
  const rewritten = scanSql(sql, (name) => {
    if (!seen.has(name)) {
      seen.set(name, names.length + 1);
      names.push(name);
    }
    return `$${seen.get(name)}`;
  });

  return names.length === 0 ? { sql, names: null } : { sql: rewritten, names };
}

module.exports = {
  loadStoolap,
  processParams,
  rewriteNamedParams,
  findLibraryPath,
};
