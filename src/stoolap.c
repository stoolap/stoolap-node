/*
 * Thin N-API module that wraps libstoolap via dlopen.
 * Uses only stable N-API (no V8-specific APIs) — one binary works across
 * all Node.js versions >= 18.
 *
 * Build: node-gyp rebuild
 * Link:  dlopen's libstoolap.{dylib,so,dll} at runtime
 */

#define NAPI_VERSION 8
#include <node_api.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifdef _WIN32
  #include <windows.h>
  #define DLOPEN(path)       LoadLibraryA(path)
  #define DLSYM(lib, name)   GetProcAddress(lib, name)
  #define DLERROR()          "LoadLibrary failed"
  typedef HMODULE LibHandle;
#else
  #include <dlfcn.h>
  #define DLOPEN(path)       dlopen(path, RTLD_NOW | RTLD_LOCAL)
  #define DLSYM(lib, name)   dlsym(lib, name)
  #define DLERROR()          dlerror()
  typedef void* LibHandle;
#endif

/* ---- Stoolap types (inlined from stoolap.h) ---- */

typedef struct StoolapDB    StoolapDB;
typedef struct StoolapStmt  StoolapStmt;
typedef struct StoolapTx    StoolapTx;
typedef struct StoolapRows  StoolapRows;

#define S_OK    0
#define S_ERR   1
#define S_ROW   100
#define S_DONE  101

#define T_NULL      0
#define T_INTEGER   1
#define T_FLOAT     2
#define T_TEXT      3
#define T_BOOLEAN   4
#define T_TIMESTAMP 5
#define T_JSON      6
#define T_BLOB      7

typedef struct {
  int32_t value_type;
  int32_t _padding;
  union {
    int64_t  integer;
    double   float64;
    int32_t  boolean;
    struct { const char* ptr; int64_t len; } text;
    struct { const uint8_t* ptr; int64_t len; } blob;
    int64_t  timestamp_nanos;
  } v;
} StoolapValue;

/* ---- Function pointer types ---- */

typedef const char* (*fn_version)(void);
typedef int32_t (*fn_open)(const char*, StoolapDB**);
typedef int32_t (*fn_open_mem)(StoolapDB**);
typedef int32_t (*fn_close)(StoolapDB*);
typedef const char* (*fn_errmsg)(const StoolapDB*);
typedef int32_t (*fn_exec)(StoolapDB*, const char*, int64_t*);
typedef int32_t (*fn_exec_p)(StoolapDB*, const char*, const StoolapValue*, int32_t, int64_t*);
typedef int32_t (*fn_query)(StoolapDB*, const char*, StoolapRows**);
typedef int32_t (*fn_query_p)(StoolapDB*, const char*, const StoolapValue*, int32_t, StoolapRows**);
typedef int32_t (*fn_prepare)(StoolapDB*, const char*, StoolapStmt**);
typedef int32_t (*fn_stmt_exec)(StoolapStmt*, const StoolapValue*, int32_t, int64_t*);
typedef int32_t (*fn_stmt_query)(StoolapStmt*, const StoolapValue*, int32_t, StoolapRows**);
typedef const char* (*fn_stmt_sql)(const StoolapStmt*);
typedef void (*fn_stmt_finalize)(StoolapStmt*);
typedef const char* (*fn_stmt_errmsg)(const StoolapStmt*);
typedef int32_t (*fn_begin)(StoolapDB*, StoolapTx**);
typedef int32_t (*fn_tx_exec)(StoolapTx*, const char*, int64_t*);
typedef int32_t (*fn_tx_exec_p)(StoolapTx*, const char*, const StoolapValue*, int32_t, int64_t*);
typedef int32_t (*fn_tx_query)(StoolapTx*, const char*, StoolapRows**);
typedef int32_t (*fn_tx_query_p)(StoolapTx*, const char*, const StoolapValue*, int32_t, StoolapRows**);
typedef int32_t (*fn_tx_commit)(StoolapTx*);
typedef int32_t (*fn_tx_rollback)(StoolapTx*);
typedef const char* (*fn_tx_errmsg)(const StoolapTx*);
typedef int32_t (*fn_rows_next)(StoolapRows*);
typedef int32_t (*fn_rows_col_count)(const StoolapRows*);
typedef const char* (*fn_rows_col_name)(const StoolapRows*, int32_t);
typedef int32_t (*fn_rows_col_type)(const StoolapRows*, int32_t);
typedef int64_t (*fn_rows_col_int)(const StoolapRows*, int32_t);
typedef double (*fn_rows_col_dbl)(const StoolapRows*, int32_t);
typedef const char* (*fn_rows_col_text)(StoolapRows*, int32_t, int64_t*);
typedef int32_t (*fn_rows_col_bool)(const StoolapRows*, int32_t);
typedef int64_t (*fn_rows_col_ts)(const StoolapRows*, int32_t);
typedef const uint8_t* (*fn_rows_col_blob)(const StoolapRows*, int32_t, int64_t*);
typedef int32_t (*fn_rows_col_null)(const StoolapRows*, int32_t);
typedef int64_t (*fn_rows_affected)(const StoolapRows*);
typedef void (*fn_rows_close)(StoolapRows*);
typedef int32_t (*fn_rows_fetch_all)(StoolapRows*, uint8_t**, int64_t*);
typedef void (*fn_buffer_free)(uint8_t*, int64_t);

/* ---- Loaded function pointers ---- */

static struct {
  fn_version version;
  fn_open open;
  fn_open_mem open_mem;
  fn_close close;
  fn_errmsg errmsg;
  fn_exec exec;
  fn_exec_p exec_p;
  fn_query query;
  fn_query_p query_p;
  fn_prepare prepare;
  fn_stmt_exec stmt_exec;
  fn_stmt_query stmt_query;
  fn_stmt_sql stmt_sql;
  fn_stmt_finalize stmt_finalize;
  fn_stmt_errmsg stmt_errmsg;
  fn_begin begin;
  fn_tx_exec tx_exec;
  fn_tx_exec_p tx_exec_p;
  fn_tx_query tx_query;
  fn_tx_query_p tx_query_p;
  fn_tx_commit tx_commit;
  fn_tx_rollback tx_rollback;
  fn_tx_errmsg tx_errmsg;
  fn_rows_next rows_next;
  fn_rows_col_count rows_col_count;
  fn_rows_col_name rows_col_name;
  fn_rows_col_type rows_col_type;
  fn_rows_col_int rows_col_int;
  fn_rows_col_dbl rows_col_dbl;
  fn_rows_col_text rows_col_text;
  fn_rows_col_bool rows_col_bool;
  fn_rows_col_ts rows_col_ts;
  fn_rows_col_blob rows_col_blob;
  fn_rows_col_null rows_col_null;
  fn_rows_affected rows_affected;
  fn_rows_close rows_close;
  fn_rows_fetch_all rows_fetch_all;
  fn_buffer_free buffer_free;
} S;

static LibHandle lib_handle = NULL;

/* ---- Helpers ---- */

#define THROW(env, msg) do { napi_throw_error(env, NULL, msg); return NULL; } while(0)
#define CHECK(env, status) if ((status) != napi_ok) return NULL

static napi_value make_changes(napi_env env, int64_t n) {
  napi_value obj, val;
  napi_create_object(env, &obj);
  if (n >= -2147483648LL && n <= 2147483647LL)
    napi_create_int32(env, (int32_t)n, &val);
  else
    napi_create_int64(env, n, &val);
  napi_set_named_property(env, obj, "changes", val);
  return obj;
}

/* Convert a JS value at index `i` in `args` array to StoolapValue. */
static int js_to_value(napi_env env, napi_value jsval, StoolapValue* out,
                       char** text_bufs, int* text_buf_count) {
  napi_valuetype vt;
  napi_typeof(env, jsval, &vt);

  if (vt == napi_undefined || vt == napi_null) {
    out->value_type = T_NULL;
    out->_padding = 0;
    out->v.integer = 0;
    return 0;
  }

  if (vt == napi_boolean) {
    bool b;
    napi_get_value_bool(env, jsval, &b);
    out->value_type = T_BOOLEAN;
    out->_padding = 0;
    out->v.boolean = b ? 1 : 0;
    return 0;
  }

  if (vt == napi_number) {
    double d;
    napi_get_value_double(env, jsval, &d);
    /* Check if integer */
    int64_t i64 = (int64_t)d;
    if ((double)i64 == d && d >= -9007199254740992.0 && d <= 9007199254740992.0) {
      out->value_type = T_INTEGER;
      out->_padding = 0;
      out->v.integer = i64;
    } else {
      out->value_type = T_FLOAT;
      out->_padding = 0;
      out->v.float64 = d;
    }
    return 0;
  }

  if (vt == napi_bigint) {
    int64_t i64;
    bool lossless;
    napi_get_value_bigint_int64(env, jsval, &i64, &lossless);
    out->value_type = T_INTEGER;
    out->_padding = 0;
    out->v.integer = i64;
    return 0;
  }

  if (vt == napi_string) {
    /* Single-copy for short strings: try stack buffer first, avoid probe call */
    char tmp[256];
    size_t len;
    napi_get_value_string_utf8(env, jsval, tmp, sizeof(tmp), &len);
    char* buf;
    if (len < sizeof(tmp) - 1) {
      buf = malloc(len + 1);
      if (!buf) return -1;
      memcpy(buf, tmp, len + 1);
    } else {
      napi_get_value_string_utf8(env, jsval, NULL, 0, &len);
      buf = malloc(len + 1);
      if (!buf) return -1;
      napi_get_value_string_utf8(env, jsval, buf, len + 1, &len);
    }
    text_bufs[*text_buf_count] = buf;
    (*text_buf_count)++;
    out->value_type = T_TEXT;
    out->_padding = 0;
    out->v.text.ptr = buf;
    out->v.text.len = (int64_t)len;
    return 0;
  }

  if (vt == napi_object) {
    /* Check Date */
    bool is_date;
    napi_is_date(env, jsval, &is_date);
    if (is_date) {
      double ms;
      napi_get_date_value(env, jsval, &ms);
      out->value_type = T_TIMESTAMP;
      out->_padding = 0;
      out->v.timestamp_nanos = (int64_t)(ms * 1000000.0);
      return 0;
    }

    /* Check TypedArray (Float32Array for vectors) */
    bool is_ta;
    napi_is_typedarray(env, jsval, &is_ta);
    if (is_ta) {
      napi_typedarray_type ta_type;
      size_t ta_len;
      void* ta_data;
      napi_get_typedarray_info(env, jsval, &ta_type, &ta_len, &ta_data, NULL, NULL);
      if (ta_type == napi_float32_array) {
        out->value_type = T_BLOB;
        out->_padding = 0;
        out->v.blob.ptr = (const uint8_t*)ta_data;
        out->v.blob.len = (int64_t)(ta_len * 4);
        return 0;
      }
    }

    /* Check Buffer */
    bool is_buf;
    napi_is_buffer(env, jsval, &is_buf);
    if (is_buf) {
      void* data;
      size_t len;
      napi_get_buffer_info(env, jsval, &data, &len);
      char* buf = malloc(len + 1);
      if (!buf) return -1;
      memcpy(buf, data, len);
      buf[len] = 0;
      text_bufs[*text_buf_count] = buf;
      (*text_buf_count)++;
      out->value_type = T_TEXT;
      out->_padding = 0;
      out->v.text.ptr = buf;
      out->v.text.len = (int64_t)len;
      return 0;
    }

    /* Object/Array → JSON */
    napi_value global, json_obj, stringify_fn, json_str;
    napi_get_global(env, &global);
    napi_get_named_property(env, global, "JSON", &json_obj);
    napi_get_named_property(env, json_obj, "stringify", &stringify_fn);
    napi_call_function(env, json_obj, stringify_fn, 1, &jsval, &json_str);

    size_t len;
    napi_get_value_string_utf8(env, json_str, NULL, 0, &len);
    char* buf = malloc(len + 1);
    if (!buf) return -1;
    napi_get_value_string_utf8(env, json_str, buf, len + 1, &len);
    text_bufs[*text_buf_count] = buf;
    (*text_buf_count)++;
    out->value_type = T_JSON;
    out->_padding = 0;
    out->v.text.ptr = buf;
    out->v.text.len = (int64_t)len;
    return 0;
  }

  out->value_type = T_NULL;
  out->_padding = 0;
  out->v.integer = 0;
  return 0;
}

/* Stack-based fast param conversion for ≤16 params.
 * Returns param count, or -1 on error. Caller must free text bufs only. */
#define MAX_STACK_PARAMS 16

/* Convert JS params (Array or null) to StoolapValue array. Caller frees. */
static int convert_params(napi_env env, napi_value js_params,
                          StoolapValue** out_values, int32_t* out_count,
                          char*** out_text_bufs, int* out_text_buf_count) {
  *out_values = NULL;
  *out_count = 0;
  *out_text_bufs = NULL;
  *out_text_buf_count = 0;

  if (!js_params) return 0;

  napi_valuetype vt;
  napi_typeof(env, js_params, &vt);
  if (vt == napi_undefined || vt == napi_null) return 0;

  bool is_array;
  napi_is_array(env, js_params, &is_array);
  if (!is_array) return 0;

  uint32_t len;
  napi_get_array_length(env, js_params, &len);
  if (len == 0) return 0;

  StoolapValue* vals = calloc(len, sizeof(StoolapValue));
  char** tbufs = calloc(len * 2, sizeof(char*)); /* generous allocation */
  int tbuf_count = 0;

  for (uint32_t i = 0; i < len; i++) {
    napi_value elem;
    napi_get_element(env, js_params, i, &elem);
    if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_count) != 0) {
      free(vals);
      for (int j = 0; j < tbuf_count; j++) free(tbufs[j]);
      free(tbufs);
      return -1;
    }
  }

  *out_values = vals;
  *out_count = (int32_t)len;
  *out_text_bufs = tbufs;
  *out_text_buf_count = tbuf_count;
  return 0;
}

static int js_to_value_copy(napi_env env, napi_value jsval, StoolapValue* out,
                            char** heap_bufs, int* heap_buf_count) {
  napi_valuetype vt;
  napi_typeof(env, jsval, &vt);

  if (vt == napi_undefined || vt == napi_null) {
    out->value_type = T_NULL;
    out->_padding = 0;
    out->v.integer = 0;
    return 0;
  }

  if (vt == napi_boolean) {
    bool b;
    napi_get_value_bool(env, jsval, &b);
    out->value_type = T_BOOLEAN;
    out->_padding = 0;
    out->v.boolean = b ? 1 : 0;
    return 0;
  }

  if (vt == napi_number) {
    double d;
    napi_get_value_double(env, jsval, &d);
    int64_t i64 = (int64_t)d;
    if ((double)i64 == d && d >= -9007199254740992.0 && d <= 9007199254740992.0) {
      out->value_type = T_INTEGER;
      out->_padding = 0;
      out->v.integer = i64;
    } else {
      out->value_type = T_FLOAT;
      out->_padding = 0;
      out->v.float64 = d;
    }
    return 0;
  }

  if (vt == napi_bigint) {
    int64_t i64;
    bool lossless;
    napi_get_value_bigint_int64(env, jsval, &i64, &lossless);
    out->value_type = T_INTEGER;
    out->_padding = 0;
    out->v.integer = i64;
    return 0;
  }

  if (vt == napi_string) {
    size_t len;
    napi_get_value_string_utf8(env, jsval, NULL, 0, &len);
    char* buf = malloc(len + 1);
    if (!buf) return -1;
    napi_get_value_string_utf8(env, jsval, buf, len + 1, &len);
    heap_bufs[*heap_buf_count] = buf;
    (*heap_buf_count)++;
    out->value_type = T_TEXT;
    out->_padding = 0;
    out->v.text.ptr = buf;
    out->v.text.len = (int64_t)len;
    return 0;
  }

  if (vt == napi_object) {
    bool is_date;
    napi_is_date(env, jsval, &is_date);
    if (is_date) {
      double ms;
      napi_get_date_value(env, jsval, &ms);
      out->value_type = T_TIMESTAMP;
      out->_padding = 0;
      out->v.timestamp_nanos = (int64_t)(ms * 1000000.0);
      return 0;
    }

    bool is_ta;
    napi_is_typedarray(env, jsval, &is_ta);
    if (is_ta) {
      napi_typedarray_type ta_type;
      size_t ta_len;
      void* ta_data;
      napi_get_typedarray_info(env, jsval, &ta_type, &ta_len, &ta_data, NULL, NULL);
      if (ta_type == napi_float32_array) {
        size_t byte_len = ta_len * 4;
        uint8_t* buf = malloc(byte_len);
        if (!buf) return -1;
        if (byte_len > 0) memcpy(buf, ta_data, byte_len);
        heap_bufs[*heap_buf_count] = (char*)buf;
        (*heap_buf_count)++;
        out->value_type = T_BLOB;
        out->_padding = 0;
        out->v.blob.ptr = buf;
        out->v.blob.len = (int64_t)byte_len;
        return 0;
      }
    }

    bool is_buf;
    napi_is_buffer(env, jsval, &is_buf);
    if (is_buf) {
      void* data;
      size_t len;
      napi_get_buffer_info(env, jsval, &data, &len);
      char* buf = malloc(len + 1);
      if (!buf) return -1;
      memcpy(buf, data, len);
      buf[len] = 0;
      heap_bufs[*heap_buf_count] = buf;
      (*heap_buf_count)++;
      out->value_type = T_TEXT;
      out->_padding = 0;
      out->v.text.ptr = buf;
      out->v.text.len = (int64_t)len;
      return 0;
    }

    napi_value global, json_obj, stringify_fn, json_str;
    napi_get_global(env, &global);
    napi_get_named_property(env, global, "JSON", &json_obj);
    napi_get_named_property(env, json_obj, "stringify", &stringify_fn);
    napi_call_function(env, json_obj, stringify_fn, 1, &jsval, &json_str);

    size_t len;
    napi_get_value_string_utf8(env, json_str, NULL, 0, &len);
    char* buf = malloc(len + 1);
    if (!buf) return -1;
    napi_get_value_string_utf8(env, json_str, buf, len + 1, &len);
    heap_bufs[*heap_buf_count] = buf;
    (*heap_buf_count)++;
    out->value_type = T_JSON;
    out->_padding = 0;
    out->v.text.ptr = buf;
    out->v.text.len = (int64_t)len;
    return 0;
  }

  out->value_type = T_NULL;
  out->_padding = 0;
  out->v.integer = 0;
  return 0;
}

static int convert_params_copy(napi_env env, napi_value js_params,
                               StoolapValue** out_values, int32_t* out_count,
                               char*** out_heap_bufs, int* out_heap_buf_count) {
  *out_values = NULL;
  *out_count = 0;
  *out_heap_bufs = NULL;
  *out_heap_buf_count = 0;

  if (!js_params) return 0;

  napi_valuetype vt;
  napi_typeof(env, js_params, &vt);
  if (vt == napi_undefined || vt == napi_null) return 0;

  bool is_array;
  napi_is_array(env, js_params, &is_array);
  if (!is_array) return 0;

  uint32_t len;
  napi_get_array_length(env, js_params, &len);
  if (len == 0) return 0;

  StoolapValue* vals = calloc(len, sizeof(StoolapValue));
  char** heap_bufs = calloc(len * 2, sizeof(char*));
  int heap_buf_count = 0;

  for (uint32_t i = 0; i < len; i++) {
    napi_value elem;
    napi_get_element(env, js_params, i, &elem);
    if (js_to_value_copy(env, elem, &vals[i], heap_bufs, &heap_buf_count) != 0) {
      free(vals);
      for (int j = 0; j < heap_buf_count; j++) free(heap_bufs[j]);
      free(heap_bufs);
      return -1;
    }
  }

  *out_values = vals;
  *out_count = (int32_t)len;
  *out_heap_bufs = heap_bufs;
  *out_heap_buf_count = heap_buf_count;
  return 0;
}

static void free_params(StoolapValue* vals, char** tbufs, int tbuf_count) {
  if (vals) free(vals);
  if (tbufs) {
    for (int i = 0; i < tbuf_count; i++) free(tbufs[i]);
    free(tbufs);
  }
}

/* ---- Constants for direct C parsing ---- */

#define MAX_DIRECT_COLS  64
#define DIRECT_ROW_LIMIT 5

/* ---- Cached prepared statement wrapper ---- */

typedef struct {
  StoolapStmt* stmt;
  int32_t col_count;
  uint32_t header_size;  /* cached byte offset to first row data in binary buffer */
  char col_names[MAX_DIRECT_COLS][128];
  int has_cache;
} CachedStmt;

static void finalize_cached_stmt(napi_env env, void* data, void* hint) {
  (void)env; (void)hint;
  CachedStmt* cs = (CachedStmt*)data;
  if (cs) {
    if (cs->stmt) S.stmt_finalize(cs->stmt);
    free(cs);
  }
}

#define GET_STMT(env, argv_0, cs_var) \
  CachedStmt* cs_var; \
  napi_get_value_external(env, argv_0, (void**)&cs_var)

/* ---- SQL string helper: single-copy for short strings ---- */

#define SQL_STACK_SIZE 512

#define GET_SQL_STRING(env, jsval, sql_var) \
  char _stack_sql[SQL_STACK_SIZE]; \
  size_t _sql_len; \
  napi_get_value_string_utf8(env, jsval, _stack_sql, SQL_STACK_SIZE, &_sql_len); \
  char* sql_var; \
  if (_sql_len < SQL_STACK_SIZE - 1) { \
    sql_var = _stack_sql; \
  } else { \
    napi_get_value_string_utf8(env, jsval, NULL, 0, &_sql_len); \
    sql_var = alloca(_sql_len + 1); \
    napi_get_value_string_utf8(env, jsval, sql_var, _sql_len + 1, &_sql_len); \
  }

/* ---- Result fetching ---- */

typedef struct {
  uint8_t* ptr;
  int64_t  len;
} RustBuffer;

static char* dup_cstr(const char* src, const char* fallback) {
  const char* use = src ? src : fallback;
  size_t len = strlen(use);
  char* copy = malloc(len + 1);
  if (!copy) return NULL;
  memcpy(copy, use, len + 1);
  return copy;
}

static void finalize_rust_buffer(napi_env env, void* data, void* hint) {
  (void)env; (void)data;
  RustBuffer* rb = (RustBuffer*)hint;
  if (rb) {
    S.buffer_free(rb->ptr, rb->len);
    free(rb);
  }
}

static napi_value make_arraybuffer_from_owned_buffer(napi_env env, uint8_t* buf, int64_t buf_len) {
  if (!buf || buf_len == 0) {
    napi_value null_val;
    napi_get_null(env, &null_val);
    if (buf) S.buffer_free(buf, buf_len);
    return null_val;
  }

  RustBuffer* rb = malloc(sizeof(RustBuffer));
  rb->ptr = buf;
  rb->len = buf_len;

  napi_value arraybuffer;
  napi_status status = napi_create_external_arraybuffer(
    env, buf, (size_t)buf_len, finalize_rust_buffer, rb, &arraybuffer);

  if (status != napi_ok) {
    free(rb);
    void* ab_data;
    napi_create_arraybuffer(env, (size_t)buf_len, &ab_data, &arraybuffer);
    memcpy(ab_data, buf, (size_t)buf_len);
    S.buffer_free(buf, buf_len);
  }

  return arraybuffer;
}

/* Return raw ArrayBuffer — used for queryRaw only */
static napi_value fetch_as_arraybuffer(napi_env env, StoolapRows* rows) {
  uint8_t* buf = NULL;
  int64_t buf_len = 0;
  int32_t rc = S.rows_fetch_all(rows, &buf, &buf_len);
  S.rows_close(rows);

  if (rc != S_OK) {
    if (buf) S.buffer_free(buf, buf_len);
    napi_value null_val;
    napi_get_null(env, &null_val);
    return null_val;
  }

  return make_arraybuffer_from_owned_buffer(env, buf, buf_len);
}

/* ---- Direct C parsing of binary buffer → JS objects ---- */

/* Read one typed value from the binary buffer, advance *poff */
static napi_value read_buf_val(napi_env env, const uint8_t* buf, uint32_t* poff) {
  napi_value val;
  uint8_t tag = buf[(*poff)++];

  switch (tag) {
    case T_NULL:
      napi_get_null(env, &val);
      break;
    case T_INTEGER: {
      int64_t v;
      memcpy(&v, buf + *poff, 8); *poff += 8;
      if (v >= -2147483648LL && v <= 2147483647LL)
        napi_create_int32(env, (int32_t)v, &val);
      else
        napi_create_int64(env, v, &val);
      break;
    }
    case T_FLOAT: {
      double d;
      memcpy(&d, buf + *poff, 8); *poff += 8;
      napi_create_double(env, d, &val);
      break;
    }
    case T_TEXT:
    case T_JSON: {
      uint32_t len;
      memcpy(&len, buf + *poff, 4); *poff += 4;
      napi_create_string_utf8(env, (const char*)(buf + *poff), len, &val);
      *poff += len;
      break;
    }
    case T_BOOLEAN:
      napi_get_boolean(env, buf[(*poff)++] != 0, &val);
      break;
    case T_TIMESTAMP: {
      int64_t nanos;
      memcpy(&nanos, buf + *poff, 8); *poff += 8;
      napi_create_date(env, (double)nanos / 1000000.0, &val);
      break;
    }
    case T_BLOB: {
      uint32_t blen;
      memcpy(&blen, buf + *poff, 4); *poff += 4;
      void* ab_data;
      napi_value ab;
      napi_create_arraybuffer(env, blen, &ab_data, &ab);
      if (blen > 0) {
        memcpy(ab_data, buf + *poff, blen);
        *poff += blen;
      }
      napi_create_typedarray(env, napi_float32_array, blen / 4, ab, 0, &val);
      break;
    }
    default:
      napi_get_null(env, &val);
  }
  return val;
}

/*
 * Read a column value directly from StoolapRows (row-by-row API).
 * No serialization — reads straight from the Rust iterator.
 * Uses rows_col_type to detect NULL (saves separate rows_col_null FFI call).
 */
static napi_value read_col_val(napi_env env, StoolapRows* rows, int32_t c) {
  napi_value val;
  int32_t type = S.rows_col_type(rows, c);
  switch (type) {
    case T_NULL:
      napi_get_null(env, &val);
      return val;
    case T_INTEGER: {
      int64_t v = S.rows_col_int(rows, c);
      if (v >= -2147483648LL && v <= 2147483647LL)
        napi_create_int32(env, (int32_t)v, &val);
      else
        napi_create_int64(env, v, &val);
      return val;
    }
    case T_FLOAT:
      napi_create_double(env, S.rows_col_dbl(rows, c), &val);
      return val;
    case T_TEXT:
    case T_JSON: {
      int64_t len;
      const char* txt = S.rows_col_text(rows, c, &len);
      napi_create_string_utf8(env, txt, (size_t)len, &val);
      return val;
    }
    case T_BOOLEAN:
      napi_get_boolean(env, S.rows_col_bool(rows, c) != 0, &val);
      return val;
    case T_TIMESTAMP: {
      double ms = (double)S.rows_col_ts(rows, c) / 1000000.0;
      napi_create_date(env, ms, &val);
      return val;
    }
    case T_BLOB: {
      int64_t blen;
      const uint8_t* bdata = S.rows_col_blob(rows, c, &blen);
      void* ab_data;
      napi_value ab;
      napi_create_arraybuffer(env, (size_t)blen, &ab_data, &ab);
      if (blen > 0) memcpy(ab_data, bdata, (size_t)blen);
      napi_create_typedarray(env, napi_float32_array, (size_t)(blen / 4), ab, 0, &val);
      return val;
    }
    default:
      napi_get_null(env, &val);
      return val;
  }
}

/*
 * Hybrid fetch: auto-selects based on result size.
 * - Small results (≤ DIRECT_ROW_LIMIT): parse buffer in C with
 *   napi_set_named_property (no napi string key creation)
 * - Large results: return ArrayBuffer for JS parsing
 * Closes rows handle.
 */
static napi_value fetch_result(napi_env env, StoolapRows* rows) {
  uint8_t* buf = NULL;
  int64_t buf_len = 0;
  int32_t rc = S.rows_fetch_all(rows, &buf, &buf_len);
  S.rows_close(rows);

  if (rc != S_OK || !buf || buf_len == 0) {
    if (buf) S.buffer_free(buf, buf_len);
    napi_value empty;
    napi_create_array_with_length(env, 0, &empty);
    return empty;
  }

  /* Parse header */
  uint32_t off = 0;
  uint32_t col_count;
  memcpy(&col_count, buf, 4); off = 4;

  /* Store null-terminated column names on stack (avoids napi string creation) */
  char col_names[MAX_DIRECT_COLS][128];
  for (uint32_t i = 0; i < col_count; i++) {
    uint16_t nlen;
    memcpy(&nlen, buf + off, 2); off += 2;
    if (i < MAX_DIRECT_COLS && nlen < 127) {
      memcpy(col_names[i], buf + off, nlen);
      col_names[i][nlen] = '\0';
    }
    off += nlen;
  }

  uint32_t row_count;
  memcpy(&row_count, buf + off, 4); off += 4;

  if (row_count <= DIRECT_ROW_LIMIT && col_count <= MAX_DIRECT_COLS) {
    /* Small result: parse in C, batch set via napi_define_properties */
    napi_value result;
    napi_create_array_with_length(env, row_count, &result);

    napi_property_descriptor props[MAX_DIRECT_COLS];
    for (uint32_t r = 0; r < row_count; r++) {
      napi_value row_obj;
      napi_create_object(env, &row_obj);
      for (uint32_t c = 0; c < col_count; c++) {
        props[c] = (napi_property_descriptor){
          .utf8name = col_names[c],
          .name = NULL, .method = NULL, .getter = NULL, .setter = NULL,
          .value = read_buf_val(env, buf, &off),
          .attributes = napi_default_jsproperty,
          .data = NULL,
        };
      }
      napi_define_properties(env, row_obj, col_count, props);
      napi_set_element(env, result, r, row_obj);
    }

    S.buffer_free(buf, buf_len);
    return result;
  }

  /* Large result: return ArrayBuffer for JS parsing */
  RustBuffer* rb = malloc(sizeof(RustBuffer));
  rb->ptr = buf;
  rb->len = buf_len;

  napi_value arraybuffer;
  napi_status status = napi_create_external_arraybuffer(
    env, buf, (size_t)buf_len, finalize_rust_buffer, rb, &arraybuffer);

  if (status != napi_ok) {
    free(rb);
    void* ab_data;
    napi_create_arraybuffer(env, (size_t)buf_len, &ab_data, &arraybuffer);
    memcpy(ab_data, buf, (size_t)buf_len);
    S.buffer_free(buf, buf_len);
  }

  return arraybuffer;
}

/*
 * Fetch first row as JS object, or null if empty.
 * Uses DIRECT row iteration — skips rows_fetch_all serialization entirely.
 * Uses napi_define_properties batch — 1 N-API call sets all properties.
 * Optimal for point lookups.
 * Closes rows handle.
 */
static napi_value fetch_one(napi_env env, StoolapRows* rows) {
  int32_t rc = S.rows_next(rows);
  if (rc != S_ROW) {
    S.rows_close(rows);
    napi_value null_val;
    napi_get_null(env, &null_val);
    return null_val;
  }

  int32_t col_count = S.rows_col_count(rows);
  if (col_count > MAX_DIRECT_COLS) col_count = MAX_DIRECT_COLS;

  napi_value row_obj;
  napi_create_object(env, &row_obj);

  /* Read all values and set in one batch via napi_define_properties */
  napi_property_descriptor props[MAX_DIRECT_COLS];
  for (int32_t c = 0; c < col_count; c++) {
    props[c] = (napi_property_descriptor){
      .utf8name = S.rows_col_name(rows, c),
      .name = NULL,
      .method = NULL,
      .getter = NULL,
      .setter = NULL,
      .value = read_col_val(env, rows, c),
      .attributes = napi_default_jsproperty,
      .data = NULL,
    };
  }
  napi_define_properties(env, row_obj, (size_t)col_count, props);

  S.rows_close(rows);
  return row_obj;
}

/*
 * Direct row iteration for prepared statement queryOne.
 * Uses rows_next + read_col_val (direct column access) — no serialization.
 * Caches column names in CachedStmt to avoid rows_col_name calls on repeat.
 * Faster than fetch_one_cached (no rows_fetch_all serialize+deserialize roundtrip).
 */
static napi_value fetch_one_direct_cached(napi_env env, StoolapRows* rows, CachedStmt* cs) {
  int32_t rc = S.rows_next(rows);
  if (rc != S_ROW) {
    S.rows_close(rows);
    napi_value null_val;
    napi_get_null(env, &null_val);
    return null_val;
  }

  int32_t col_count;

  if (!cs->has_cache) {
    /* First call: cache column names from rows iterator */
    int32_t cc = S.rows_col_count(rows);
    col_count = (cc > MAX_DIRECT_COLS) ? MAX_DIRECT_COLS : cc;
    cs->col_count = col_count;

    for (int32_t i = 0; i < col_count; i++) {
      const char* name = S.rows_col_name(rows, i);
      size_t nlen = strlen(name);
      if (nlen > 127) nlen = 127;
      memcpy(cs->col_names[i], name, nlen);
      cs->col_names[i][nlen] = '\0';
    }
    cs->has_cache = 1;
  } else {
    col_count = cs->col_count;
  }

  napi_value row_obj;
  napi_create_object(env, &row_obj);

  napi_property_descriptor props[MAX_DIRECT_COLS];
  for (int32_t c = 0; c < col_count; c++) {
    props[c] = (napi_property_descriptor){
      .utf8name = cs->col_names[c],
      .name = NULL,
      .method = NULL,
      .getter = NULL,
      .setter = NULL,
      .value = read_col_val(env, rows, c),
      .attributes = napi_default_jsproperty,
      .data = NULL,
    };
  }
  napi_define_properties(env, row_obj, (size_t)col_count, props);

  S.rows_close(rows);
  return row_obj;
}

/*
 * Hybrid fetch for prepared statements with column name caching.
 * Same as fetch_result but uses CachedStmt to cache header info.
 * On repeat calls: skip header parsing, use cached column names.
 */
static napi_value fetch_result_cached(napi_env env, StoolapRows* rows, CachedStmt* cs) {
  uint8_t* buf = NULL;
  int64_t buf_len = 0;
  int32_t rc = S.rows_fetch_all(rows, &buf, &buf_len);
  S.rows_close(rows);

  if (rc != S_OK || !buf || buf_len == 0) {
    if (buf) S.buffer_free(buf, buf_len);
    napi_value empty;
    napi_create_array_with_length(env, 0, &empty);
    return empty;
  }

  uint32_t off = 0;
  int32_t col_count;

  if (!cs->has_cache || cs->header_size == 0) {
    /* First call, or cache was set by direct path (no header_size) */
    uint32_t cc;
    memcpy(&cc, buf, 4); off = 4;
    col_count = (cc > MAX_DIRECT_COLS) ? MAX_DIRECT_COLS : (int32_t)cc;
    cs->col_count = col_count;

    for (int32_t i = 0; i < col_count; i++) {
      uint16_t nlen;
      memcpy(&nlen, buf + off, 2); off += 2;
      uint16_t cplen = (nlen > 127) ? 127 : nlen;
      memcpy(cs->col_names[i], buf + off, cplen);
      cs->col_names[i][cplen] = '\0';
      off += nlen;
    }

    uint32_t row_count;
    memcpy(&row_count, buf + off, 4); off += 4;
    cs->header_size = off;
    cs->has_cache = 1;

    if (row_count == 0) {
      S.buffer_free(buf, buf_len);
      napi_value empty;
      napi_create_array_with_length(env, 0, &empty);
      return empty;
    }

    if (row_count <= DIRECT_ROW_LIMIT) {
      /* Small result: parse in C */
      napi_value result;
      napi_create_array_with_length(env, row_count, &result);
      napi_property_descriptor props[MAX_DIRECT_COLS];
      for (uint32_t r = 0; r < row_count; r++) {
        napi_value row_obj;
        napi_create_object(env, &row_obj);
        for (int32_t c = 0; c < col_count; c++) {
          props[c] = (napi_property_descriptor){
            .utf8name = cs->col_names[c],
            .name = NULL, .method = NULL, .getter = NULL, .setter = NULL,
            .value = read_buf_val(env, buf, &off),
            .attributes = napi_default_jsproperty, .data = NULL,
          };
        }
        napi_define_properties(env, row_obj, (size_t)col_count, props);
        napi_set_element(env, result, r, row_obj);
      }
      S.buffer_free(buf, buf_len);
      return result;
    }
  } else {
    /* Cached: use cached column info */
    col_count = cs->col_count;
    off = cs->header_size;
    uint32_t row_count;
    memcpy(&row_count, buf + cs->header_size - 4, 4);

    if (row_count == 0) {
      S.buffer_free(buf, buf_len);
      napi_value empty;
      napi_create_array_with_length(env, 0, &empty);
      return empty;
    }

    if (row_count <= DIRECT_ROW_LIMIT) {
      napi_value result;
      napi_create_array_with_length(env, row_count, &result);
      napi_property_descriptor props[MAX_DIRECT_COLS];
      for (uint32_t r = 0; r < row_count; r++) {
        napi_value row_obj;
        napi_create_object(env, &row_obj);
        for (int32_t c = 0; c < col_count; c++) {
          props[c] = (napi_property_descriptor){
            .utf8name = cs->col_names[c],
            .name = NULL, .method = NULL, .getter = NULL, .setter = NULL,
            .value = read_buf_val(env, buf, &off),
            .attributes = napi_default_jsproperty, .data = NULL,
          };
        }
        napi_define_properties(env, row_obj, (size_t)col_count, props);
        napi_set_element(env, result, r, row_obj);
      }
      S.buffer_free(buf, buf_len);
      return result;
    }
  }

  /* Large result: return ArrayBuffer for JS parsing */
  RustBuffer* rb = malloc(sizeof(RustBuffer));
  rb->ptr = buf;
  rb->len = buf_len;

  napi_value arraybuffer;
  napi_status status = napi_create_external_arraybuffer(
    env, buf, (size_t)buf_len, finalize_rust_buffer, rb, &arraybuffer);

  if (status != napi_ok) {
    free(rb);
    void* ab_data;
    napi_create_arraybuffer(env, (size_t)buf_len, &ab_data, &arraybuffer);
    memcpy(ab_data, buf, (size_t)buf_len);
    S.buffer_free(buf, buf_len);
  }

  return arraybuffer;
}

/* Forward declaration for bound function used in dbPrepare */
static napi_value wrap_stmt_query_one_int_bound(napi_env env, napi_callback_info info);

/* ---- Async work helpers ---- */

typedef struct {
  napi_async_work work;
  napi_deferred deferred;
  char* error;
} AsyncBase;

typedef struct {
  AsyncBase base;
  char* dsn;
  StoolapDB* db;
} AsyncDbOpenTask;

typedef struct {
  AsyncBase base;
  StoolapDB* db;
} AsyncDbCloseTask;

typedef enum {
  ASYNC_EXEC_DB,
  ASYNC_EXEC_DB_SIMPLE,
  ASYNC_EXEC_TX,
  ASYNC_EXEC_STMT,
} AsyncExecKind;

typedef struct {
  AsyncBase base;
  AsyncExecKind kind;
  void* target;
  char* sql;
  StoolapValue* vals;
  int32_t val_count;
  char** heap_bufs;
  int heap_buf_count;
  int64_t affected;
} AsyncExecTask;

typedef enum {
  ASYNC_QUERY_DB,
  ASYNC_QUERY_TX,
  ASYNC_QUERY_STMT,
} AsyncQueryKind;

typedef struct {
  AsyncBase base;
  AsyncQueryKind kind;
  void* target;
  char* sql;
  StoolapValue* vals;
  int32_t val_count;
  char** heap_bufs;
  int heap_buf_count;
  uint8_t* buf;
  int64_t buf_len;
} AsyncQueryTask;

typedef struct {
  AsyncBase base;
  StoolapDB* db;
  StoolapTx* tx;
} AsyncTxBeginTask;

typedef enum {
  ASYNC_TX_COMMIT,
  ASYNC_TX_ROLLBACK,
} AsyncTxFinishKind;

typedef struct {
  AsyncBase base;
  AsyncTxFinishKind kind;
  StoolapTx* tx;
} AsyncTxFinishTask;

static int copy_napi_string(napi_env env, napi_value jsval, char** out) {
  size_t len = 0;
  *out = NULL;
  napi_get_value_string_utf8(env, jsval, NULL, 0, &len);
  char* buf = malloc(len + 1);
  if (!buf) return -1;
  napi_get_value_string_utf8(env, jsval, buf, len + 1, &len);
  *out = buf;
  return 0;
}

static void reject_async_error(napi_env env, AsyncBase* base, const char* fallback) {
  napi_value msg, err;
  const char* text = base->error ? base->error : fallback;
  napi_create_string_utf8(env, text, NAPI_AUTO_LENGTH, &msg);
  napi_create_error(env, NULL, msg, &err);
  napi_reject_deferred(env, base->deferred, err);
}

static napi_value queue_async_work(napi_env env, void* task, AsyncBase* base,
                                   const char* name,
                                   napi_async_execute_callback execute,
                                   napi_async_complete_callback complete) {
  napi_value promise, resource_name;
  if (napi_create_promise(env, &base->deferred, &promise) != napi_ok) return NULL;
  if (napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &resource_name) != napi_ok) return NULL;
  if (napi_create_async_work(env, NULL, resource_name, execute, complete, task, &base->work) != napi_ok) {
    return NULL;
  }
  if (napi_queue_async_work(env, base->work) != napi_ok) {
    napi_delete_async_work(env, base->work);
    return NULL;
  }
  return promise;
}

static void execute_db_open_async(napi_env env, void* data) {
  (void)env;
  AsyncDbOpenTask* task = (AsyncDbOpenTask*)data;
  int32_t rc;

  if (task->dsn[0] == '\0' ||
      strcmp(task->dsn, ":memory:") == 0 ||
      strcmp(task->dsn, "memory://") == 0) {
    rc = S.open_mem(&task->db);
  } else {
    rc = S.open(task->dsn, &task->db);
  }

  if (rc != S_OK) {
    task->base.error = dup_cstr(S.errmsg(NULL), "Failed to open database");
  }
}

static void complete_db_open_async(napi_env env, napi_status status, void* data) {
  AsyncDbOpenTask* task = (AsyncDbOpenTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Database open cancelled");
  }

  if (task->base.error) {
    if (task->db) {
      S.close(task->db);
      task->db = NULL;
    }
    reject_async_error(env, &task->base, "Failed to open database");
  } else {
    napi_value ext;
    napi_create_external(env, task->db, NULL, NULL, &ext);
    napi_resolve_deferred(env, task->base.deferred, ext);
  }

  napi_delete_async_work(env, task->base.work);
  free(task->dsn);
  free(task->base.error);
  free(task);
}

static void execute_db_close_async(napi_env env, void* data) {
  (void)env;
  AsyncDbCloseTask* task = (AsyncDbCloseTask*)data;
  if (task->db) S.close(task->db);
}

static void complete_db_close_async(napi_env env, napi_status status, void* data) {
  AsyncDbCloseTask* task = (AsyncDbCloseTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Database close cancelled");
  }

  if (task->base.error) {
    reject_async_error(env, &task->base, "Failed to close database");
  } else {
    napi_value undef;
    napi_get_undefined(env, &undef);
    napi_resolve_deferred(env, task->base.deferred, undef);
  }

  napi_delete_async_work(env, task->base.work);
  free(task->base.error);
  free(task);
}

static void execute_async_exec(napi_env env, void* data) {
  (void)env;
  AsyncExecTask* task = (AsyncExecTask*)data;
  int32_t rc = S_ERR;

  switch (task->kind) {
    case ASYNC_EXEC_DB: {
      StoolapDB* db = (StoolapDB*)task->target;
      rc = task->val_count > 0
        ? S.exec_p(db, task->sql, task->vals, task->val_count, &task->affected)
        : S.exec(db, task->sql, &task->affected);
      if (rc != S_OK) task->base.error = dup_cstr(S.errmsg(db), "Exec error");
      break;
    }
    case ASYNC_EXEC_DB_SIMPLE: {
      StoolapDB* db = (StoolapDB*)task->target;
      rc = S.exec(db, task->sql, &task->affected);
      if (rc != S_OK) task->base.error = dup_cstr(S.errmsg(db), "Exec error");
      break;
    }
    case ASYNC_EXEC_TX: {
      StoolapTx* tx = (StoolapTx*)task->target;
      rc = task->val_count > 0
        ? S.tx_exec_p(tx, task->sql, task->vals, task->val_count, &task->affected)
        : S.tx_exec(tx, task->sql, &task->affected);
      if (rc != S_OK) task->base.error = dup_cstr(S.tx_errmsg(tx), "Transaction exec error");
      break;
    }
    case ASYNC_EXEC_STMT: {
      StoolapStmt* stmt = (StoolapStmt*)task->target;
      rc = S.stmt_exec(stmt, task->vals, task->val_count, &task->affected);
      if (rc != S_OK) task->base.error = dup_cstr(S.stmt_errmsg(stmt), "Statement exec error");
      break;
    }
  }
}

static void complete_async_exec(napi_env env, napi_status status, void* data) {
  AsyncExecTask* task = (AsyncExecTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Async exec cancelled");
  }

  if (task->base.error) {
    reject_async_error(env, &task->base, "Exec error");
  } else if (task->kind == ASYNC_EXEC_DB_SIMPLE) {
    napi_value undef;
    napi_get_undefined(env, &undef);
    napi_resolve_deferred(env, task->base.deferred, undef);
  } else {
    napi_resolve_deferred(env, task->base.deferred, make_changes(env, task->affected));
  }

  napi_delete_async_work(env, task->base.work);
  free(task->sql);
  free_params(task->vals, task->heap_bufs, task->heap_buf_count);
  free(task->base.error);
  free(task);
}

static void execute_async_query(napi_env env, void* data) {
  (void)env;
  AsyncQueryTask* task = (AsyncQueryTask*)data;
  StoolapRows* rows = NULL;
  int32_t rc = S_ERR;

  if (task->kind == ASYNC_QUERY_DB) {
    StoolapDB* db = (StoolapDB*)task->target;
    rc = task->val_count > 0
      ? S.query_p(db, task->sql, task->vals, task->val_count, &rows)
      : S.query(db, task->sql, &rows);
    if (rc != S_OK) {
      task->base.error = dup_cstr(S.errmsg(db), "Query error");
      return;
    }
  } else if (task->kind == ASYNC_QUERY_TX) {
    StoolapTx* tx = (StoolapTx*)task->target;
    rc = task->val_count > 0
      ? S.tx_query_p(tx, task->sql, task->vals, task->val_count, &rows)
      : S.tx_query(tx, task->sql, &rows);
    if (rc != S_OK) {
      task->base.error = dup_cstr(S.tx_errmsg(tx), "Transaction query error");
      return;
    }
  } else {
    StoolapStmt* stmt = (StoolapStmt*)task->target;
    rc = S.stmt_query(stmt, task->vals, task->val_count, &rows);
    if (rc != S_OK) {
      task->base.error = dup_cstr(S.stmt_errmsg(stmt), "Statement query error");
      return;
    }
  }

  rc = S.rows_fetch_all(rows, &task->buf, &task->buf_len);
  S.rows_close(rows);
  if (rc != S_OK) {
    task->base.error = dup_cstr(NULL, "Failed to fetch query results");
    if (task->buf) {
      S.buffer_free(task->buf, task->buf_len);
      task->buf = NULL;
      task->buf_len = 0;
    }
  }
}

static void complete_async_query(napi_env env, napi_status status, void* data) {
  AsyncQueryTask* task = (AsyncQueryTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Async query cancelled");
  }

  if (task->base.error) {
    if (task->buf) {
      S.buffer_free(task->buf, task->buf_len);
      task->buf = NULL;
      task->buf_len = 0;
    }
    reject_async_error(env, &task->base, "Query error");
  } else {
    napi_resolve_deferred(env, task->base.deferred,
                          make_arraybuffer_from_owned_buffer(env, task->buf, task->buf_len));
    task->buf = NULL;
    task->buf_len = 0;
  }

  napi_delete_async_work(env, task->base.work);
  free(task->sql);
  free_params(task->vals, task->heap_bufs, task->heap_buf_count);
  free(task->base.error);
  free(task);
}

static void execute_tx_begin_async(napi_env env, void* data) {
  (void)env;
  AsyncTxBeginTask* task = (AsyncTxBeginTask*)data;
  int32_t rc = S.begin(task->db, &task->tx);
  if (rc != S_OK) {
    task->base.error = dup_cstr(S.errmsg(task->db), "Begin error");
  }
}

static void complete_tx_begin_async(napi_env env, napi_status status, void* data) {
  AsyncTxBeginTask* task = (AsyncTxBeginTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Transaction begin cancelled");
  }

  if (task->base.error) {
    reject_async_error(env, &task->base, "Begin error");
  } else {
    napi_value ext;
    napi_create_external(env, task->tx, NULL, NULL, &ext);
    napi_resolve_deferred(env, task->base.deferred, ext);
  }

  napi_delete_async_work(env, task->base.work);
  free(task->base.error);
  free(task);
}

static void execute_tx_finish_async(napi_env env, void* data) {
  (void)env;
  AsyncTxFinishTask* task = (AsyncTxFinishTask*)data;
  int32_t rc = task->kind == ASYNC_TX_COMMIT
    ? S.tx_commit(task->tx)
    : S.tx_rollback(task->tx);

  if (rc != S_OK) {
    task->base.error = dup_cstr(S.tx_errmsg(task->tx),
                                task->kind == ASYNC_TX_COMMIT
                                  ? "Failed to commit transaction"
                                  : "Failed to rollback transaction");
  }
}

static void complete_tx_finish_async(napi_env env, napi_status status, void* data) {
  AsyncTxFinishTask* task = (AsyncTxFinishTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL,
                                task->kind == ASYNC_TX_COMMIT
                                  ? "Transaction commit cancelled"
                                  : "Transaction rollback cancelled");
  }

  if (task->base.error) {
    reject_async_error(env, &task->base,
                       task->kind == ASYNC_TX_COMMIT
                         ? "Failed to commit transaction"
                         : "Failed to rollback transaction");
  } else {
    napi_value undef;
    napi_get_undefined(env, &undef);
    napi_resolve_deferred(env, task->base.deferred, undef);
  }

  napi_delete_async_work(env, task->base.work);
  free(task->base.error);
  free(task);
}

/* ============================================================
 * Exported N-API functions
 * ============================================================ */

/* loadLibrary(path: string): void */
static napi_value fn_load_library(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  char path[4096];
  napi_get_value_string_utf8(env, argv[0], path, sizeof(path), NULL);

  lib_handle = DLOPEN(path);
  if (!lib_handle) THROW(env, DLERROR());

  #define LOAD(field, name) do { \
    void* _sym = DLSYM(lib_handle, name); \
    if (!_sym) THROW(env, "Missing symbol: " name); \
    *(void**)&S.field = _sym; \
  } while(0)

  LOAD(version,       "stoolap_version");
  LOAD(open,          "stoolap_open");
  LOAD(open_mem,      "stoolap_open_in_memory");
  LOAD(close,         "stoolap_close");
  LOAD(errmsg,        "stoolap_errmsg");
  LOAD(exec,          "stoolap_exec");
  LOAD(exec_p,        "stoolap_exec_params");
  LOAD(query,         "stoolap_query");
  LOAD(query_p,       "stoolap_query_params");
  LOAD(prepare,       "stoolap_prepare");
  LOAD(stmt_exec,     "stoolap_stmt_exec");
  LOAD(stmt_query,    "stoolap_stmt_query");
  LOAD(stmt_sql,      "stoolap_stmt_sql");
  LOAD(stmt_finalize, "stoolap_stmt_finalize");
  LOAD(stmt_errmsg,   "stoolap_stmt_errmsg");
  LOAD(begin,         "stoolap_begin");
  LOAD(tx_exec,       "stoolap_tx_exec");
  LOAD(tx_exec_p,     "stoolap_tx_exec_params");
  LOAD(tx_query,      "stoolap_tx_query");
  LOAD(tx_query_p,    "stoolap_tx_query_params");
  LOAD(tx_commit,     "stoolap_tx_commit");
  LOAD(tx_rollback,   "stoolap_tx_rollback");
  LOAD(tx_errmsg,     "stoolap_tx_errmsg");
  LOAD(rows_next,     "stoolap_rows_next");
  LOAD(rows_col_count,"stoolap_rows_column_count");
  LOAD(rows_col_name, "stoolap_rows_column_name");
  LOAD(rows_col_type, "stoolap_rows_column_type");
  LOAD(rows_col_int,  "stoolap_rows_column_int64");
  LOAD(rows_col_dbl,  "stoolap_rows_column_double");
  LOAD(rows_col_text, "stoolap_rows_column_text");
  LOAD(rows_col_bool, "stoolap_rows_column_bool");
  LOAD(rows_col_ts,   "stoolap_rows_column_timestamp");
  LOAD(rows_col_blob, "stoolap_rows_column_blob");
  LOAD(rows_col_null, "stoolap_rows_column_is_null");
  LOAD(rows_affected, "stoolap_rows_affected");
  LOAD(rows_close,    "stoolap_rows_close");
  LOAD(rows_fetch_all,"stoolap_rows_fetch_all");
  LOAD(buffer_free,   "stoolap_buffer_free");

  #undef LOAD

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* dbOpen(dsn: string): external */
static napi_value fn_db_open(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  char dsn[4096];
  size_t dsn_len;
  napi_get_value_string_utf8(env, argv[0], dsn, sizeof(dsn), &dsn_len);

  StoolapDB* db = NULL;
  int32_t rc;

  if (dsn_len == 0 || strcmp(dsn, ":memory:") == 0 || strcmp(dsn, "memory://") == 0) {
    rc = S.open_mem(&db);
  } else {
    rc = S.open(dsn, &db);
  }

  if (rc != S_OK) {
    const char* msg = S.errmsg(NULL);
    THROW(env, msg ? msg : "Failed to open database");
  }

  napi_value ext;
  napi_create_external(env, db, NULL, NULL, &ext);
  return ext;
}

/* dbClose(external): void */
static napi_value fn_db_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  if (db) S.close(db);

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* dbOpenAsync(dsn: string): Promise<external> */
static napi_value fn_db_open_async(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncDbOpenTask* task = calloc(1, sizeof(AsyncDbOpenTask));
  if (!task) THROW(env, "Out of memory");
  if (copy_napi_string(env, argv[0], &task->dsn) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "dbOpenAsync", execute_db_open_async, complete_db_open_async);
  if (!promise) {
    free(task->dsn);
    free(task);
    THROW(env, "Failed to queue dbOpenAsync");
  }
  return promise;
}

/* dbCloseAsync(external): Promise<void> */
static napi_value fn_db_close_async(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncDbCloseTask* task = calloc(1, sizeof(AsyncDbCloseTask));
  if (!task) THROW(env, "Out of memory");
  napi_get_value_external(env, argv[0], (void**)&task->db);

  napi_value promise = queue_async_work(
    env, task, &task->base, "dbCloseAsync", execute_db_close_async, complete_db_close_async);
  if (!promise) {
    free(task);
    THROW(env, "Failed to queue dbCloseAsync");
  }
  return promise;
}

/* dbExec(external, sql, params?): { changes } */
static napi_value fn_db_exec(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  int64_t affected = 0;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.exec(db, sql, &affected);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.exec(db, sql, &affected);
      } else if (len <= MAX_STACK_PARAMS) {
        /* Stack-allocated fast path */
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.exec_p(db, sql, vals, (int32_t)len, &affected);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.exec_p(db, sql, vals, cnt, &affected);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.exec(db, sql, &affected);
  }

  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Exec error");
  }

  return make_changes(env, affected);
}

/* dbExecAsync(external, sql, params?): Promise<{ changes }> */
static napi_value fn_db_exec_async(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncExecTask* task = calloc(1, sizeof(AsyncExecTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_EXEC_DB;
  napi_get_value_external(env, argv[0], &task->target);
  if (copy_napi_string(env, argv[1], &task->sql) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  if (argc > 2) {
    if (convert_params_copy(env, argv[2], &task->vals, &task->val_count,
                            &task->heap_bufs, &task->heap_buf_count) != 0) {
      free(task->sql);
      free(task);
      THROW(env, "Failed to convert parameters");
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "dbExecAsync", execute_async_exec, complete_async_exec);
  if (!promise) {
    free(task->sql);
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue dbExecAsync");
  }
  return promise;
}

/* dbExecSimple(external, sql): void — no params, no return */
static napi_value fn_db_exec_simple(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  int64_t affected = 0;
  int32_t rc = S.exec(db, sql, &affected);
  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Exec error");
  }

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* dbExecSimpleAsync(external, sql): Promise<void> */
static napi_value fn_db_exec_simple_async(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncExecTask* task = calloc(1, sizeof(AsyncExecTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_EXEC_DB_SIMPLE;
  napi_get_value_external(env, argv[0], &task->target);
  if (copy_napi_string(env, argv[1], &task->sql) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "dbExecSimpleAsync", execute_async_exec, complete_async_exec);
  if (!promise) {
    free(task->sql);
    free(task);
    THROW(env, "Failed to queue dbExecSimpleAsync");
  }
  return promise;
}

/* dbQueryBuf(external, sql, params?): ArrayBuffer | null */
static napi_value fn_db_query_buf(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.query(db, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.query(db, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.query_p(db, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.query_p(db, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.query(db, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }

  return fetch_as_arraybuffer(env, rows);
}

/* dbQueryBufAsync(external, sql, params?): Promise<ArrayBuffer | null> */
static napi_value fn_db_query_buf_async(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncQueryTask* task = calloc(1, sizeof(AsyncQueryTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_QUERY_DB;
  napi_get_value_external(env, argv[0], &task->target);
  if (copy_napi_string(env, argv[1], &task->sql) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  if (argc > 2) {
    if (convert_params_copy(env, argv[2], &task->vals, &task->val_count,
                            &task->heap_bufs, &task->heap_buf_count) != 0) {
      free(task->sql);
      free(task);
      THROW(env, "Failed to convert parameters");
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "dbQueryBufAsync", execute_async_query, complete_async_query);
  if (!promise) {
    free(task->sql);
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue dbQueryBufAsync");
  }
  return promise;
}

/* dbQuery(external, sql, params?): Array | ArrayBuffer */
static napi_value fn_db_query(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.query(db, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.query(db, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.query_p(db, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.query_p(db, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.query(db, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }

  return fetch_result(env, rows);
}

/* dbQueryOne(external, sql, params?): Object | null */
static napi_value fn_db_query_one(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.query(db, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.query(db, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.query_p(db, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.query_p(db, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.query(db, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }

  return fetch_one(env, rows);
}

/* dbPrepare(external, sql): external (returns CachedStmt wrapper) */
static napi_value fn_db_prepare(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapStmt* stmt = NULL;
  int32_t rc = S.prepare(db, sql, &stmt);
  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Prepare error");
  }

  CachedStmt* cs = calloc(1, sizeof(CachedStmt));
  cs->stmt = stmt;
  cs->has_cache = 0;

  napi_value ext;
  napi_create_external(env, cs, finalize_cached_stmt, NULL, &ext);

  /* Create bound queryOneInt function with CachedStmt baked in */
  napi_value bound_fn;
  napi_create_function(env, "q1i", NAPI_AUTO_LENGTH,
                       wrap_stmt_query_one_int_bound, cs, &bound_fn);

  /* Return [external, boundFn] */
  napi_value result;
  napi_create_array_with_length(env, 2, &result);
  napi_set_element(env, result, 0, ext);
  napi_set_element(env, result, 1, bound_fn);
  return result;
}

/* stmtExec(external, params?): { changes }
 * Uses stack allocation for ≤16 params (avoids calloc/free overhead). */
static napi_value wrap_stmt_exec(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  if (argc <= 1) {
    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, NULL, 0, &affected);
    if (rc != S_OK) {
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement exec error");
    }
    return make_changes(env, affected);
  }

  /* Check if params is null/undefined */
  napi_valuetype vt;
  napi_typeof(env, argv[1], &vt);
  if (vt == napi_undefined || vt == napi_null) {
    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, NULL, 0, &affected);
    if (rc != S_OK) {
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement exec error");
    }
    return make_changes(env, affected);
  }

  uint32_t len;
  napi_get_array_length(env, argv[1], &len);

  if (len == 0) {
    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, NULL, 0, &affected);
    if (rc != S_OK) {
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement exec error");
    }
    return make_changes(env, affected);
  }

  if (len <= MAX_STACK_PARAMS) {
    /* Stack-allocated fast path — no calloc/free for vals and tbufs */
    StoolapValue vals[MAX_STACK_PARAMS];
    char* tbufs[MAX_STACK_PARAMS * 2];
    int tbuf_cnt = 0;

    for (uint32_t i = 0; i < len; i++) {
      napi_value elem;
      napi_get_element(env, argv[1], i, &elem);
      if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
        THROW(env, "Failed to convert parameters");
      }
    }

    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, vals, (int32_t)len, &affected);
    for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);

    if (rc != S_OK) {
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement exec error");
    }
    return make_changes(env, affected);
  }

  /* Large param count — heap allocated */
  StoolapValue* vals = NULL; int32_t cnt = 0;
  char** tbufs = NULL; int tbuf_cnt = 0;
  if (convert_params(env, argv[1], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
    THROW(env, "Failed to convert parameters");

  int64_t affected = 0;
  int32_t rc = S.stmt_exec(stmt, vals, cnt, &affected);
  free_params(vals, tbufs, tbuf_cnt);

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(stmt);
    THROW(env, msg ? msg : "Statement exec error");
  }

  return make_changes(env, affected);
}

/* stmtQueryBuf(external, params?): ArrayBuffer | null */
static napi_value wrap_stmt_query_buf(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc <= 1) {
    rc = S.stmt_query(stmt, NULL, 0, &rows);
  } else {
    napi_valuetype vt;
    napi_typeof(env, argv[1], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.stmt_query(stmt, NULL, 0, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[1], &len);
      if (len == 0) {
        rc = S.stmt_query(stmt, NULL, 0, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[1], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.stmt_query(stmt, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals = NULL; int32_t cnt = 0;
        char** tbufs = NULL; int tbuf_cnt = 0;
        if (convert_params(env, argv[1], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.stmt_query(stmt, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  }

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_as_arraybuffer(env, rows);
}

/* stmtExecAsync(external, params?): Promise<{ changes }> */
static napi_value wrap_stmt_exec_async(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);

  AsyncExecTask* task = calloc(1, sizeof(AsyncExecTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_EXEC_STMT;
  task->target = cs->stmt;
  task->sql = NULL;

  if (argc > 1) {
    napi_valuetype vt;
    napi_typeof(env, argv[1], &vt);
    if (vt != napi_undefined && vt != napi_null) {
      if (convert_params_copy(env, argv[1], &task->vals, &task->val_count,
                              &task->heap_bufs, &task->heap_buf_count) != 0) {
        free(task);
        THROW(env, "Failed to convert parameters");
      }
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "stmtExecAsync", execute_async_exec, complete_async_exec);
  if (!promise) {
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue stmtExecAsync");
  }
  return promise;
}

/* stmtQueryBufAsync(external, params?): Promise<ArrayBuffer> */
static napi_value wrap_stmt_query_buf_async(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);

  AsyncQueryTask* task = calloc(1, sizeof(AsyncQueryTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_QUERY_STMT;
  task->target = cs->stmt;
  task->sql = NULL;

  if (argc > 1) {
    napi_valuetype vt;
    napi_typeof(env, argv[1], &vt);
    if (vt != napi_undefined && vt != napi_null) {
      if (convert_params_copy(env, argv[1], &task->vals, &task->val_count,
                              &task->heap_bufs, &task->heap_buf_count) != 0) {
        free(task);
        THROW(env, "Failed to convert parameters");
      }
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "stmtQueryBufAsync", execute_async_query, complete_async_query);
  if (!promise) {
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue stmtQueryBufAsync");
  }
  return promise;
}

/* stmtQuery(external, params?): Array | ArrayBuffer
 * Uses stack allocation for ≤16 params. */
static napi_value wrap_stmt_query(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc <= 1) {
    rc = S.stmt_query(stmt, NULL, 0, &rows);
  } else {
    napi_valuetype vt;
    napi_typeof(env, argv[1], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.stmt_query(stmt, NULL, 0, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[1], &len);
      if (len == 0) {
        rc = S.stmt_query(stmt, NULL, 0, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[1], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.stmt_query(stmt, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals = NULL; int32_t cnt = 0;
        char** tbufs = NULL; int tbuf_cnt = 0;
        if (convert_params(env, argv[1], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.stmt_query(stmt, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  }

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_result_cached(env, rows, cs);
}

/* stmtQueryOne(external, params?): Object | null
 * Uses stack allocation for ≤16 params. */
static napi_value wrap_stmt_query_one(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc <= 1) {
    rc = S.stmt_query(stmt, NULL, 0, &rows);
  } else {
    napi_valuetype vt;
    napi_typeof(env, argv[1], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.stmt_query(stmt, NULL, 0, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[1], &len);
      if (len == 0) {
        rc = S.stmt_query(stmt, NULL, 0, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[1], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.stmt_query(stmt, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals = NULL; int32_t cnt = 0;
        char** tbufs = NULL; int tbuf_cnt = 0;
        if (convert_params(env, argv[1], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.stmt_query(stmt, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  }

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_one_direct_cached(env, rows, cs);
}

/* stmtQueryOneIntBound(intValue): Object | null
 * Bound version — CachedStmt baked into function data.
 * Eliminates napi_get_value_external call (~35ns savings per call). */
static napi_value wrap_stmt_query_one_int_bound(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  void* data;
  napi_get_cb_info(env, info, &argc, argv, NULL, &data);
  CachedStmt* cs = (CachedStmt*)data;

  double d;
  napi_get_value_double(env, argv[0], &d);
  StoolapValue val;
  val.value_type = T_INTEGER;
  val._padding = 0;
  val.v.integer = (int64_t)d;

  StoolapRows* rows = NULL;
  int32_t rc = S.stmt_query(cs->stmt, &val, 1, &rows);

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_one_direct_cached(env, rows, cs);
}

/* Helper: read numeric args from argv[start..start+count] into StoolapValue array */
static void read_num_args(napi_env env, napi_value* argv, uint32_t start, uint32_t count, StoolapValue* vals) {
  for (uint32_t i = 0; i < count; i++) {
    double d;
    napi_get_value_double(env, argv[start + i], &d);
    int64_t i64 = (int64_t)d;
    if ((double)i64 == d && d >= -9007199254740992.0 && d <= 9007199254740992.0) {
      vals[i].value_type = T_INTEGER;
      vals[i]._padding = 0;
      vals[i].v.integer = i64;
    } else {
      vals[i].value_type = T_FLOAT;
      vals[i]._padding = 0;
      vals[i].v.float64 = d;
    }
  }
}

/* stmtExecNums(external, num1, num2, ...): int
 * Direct numeric args — no array unwrapping, returns raw int (JS wraps in {changes}).
 * Saves: napi_get_array_length + N×napi_get_element + napi_create_object + napi_set_named_property */
static napi_value wrap_stmt_exec_nums(napi_env env, napi_callback_info info) {
  size_t argc = 17;
  napi_value argv[17];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  uint32_t len = (uint32_t)(argc - 1);
  if (len > 16) len = 16;

  StoolapValue vals[16];
  read_num_args(env, argv, 1, len, vals);

  int64_t affected = 0;
  int32_t rc = S.stmt_exec(cs->stmt, vals, (int32_t)len, &affected);

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement exec error");
  }

  napi_value result;
  napi_create_int32(env, (int32_t)affected, &result);
  return result;
}

/* stmtQueryNums(external, num1, num2, ...): Array | ArrayBuffer
 * Direct numeric args — no array unwrapping overhead. */
static napi_value wrap_stmt_query_nums(napi_env env, napi_callback_info info) {
  size_t argc = 17;
  napi_value argv[17];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  uint32_t len = (uint32_t)(argc - 1);
  if (len > 16) len = 16;

  StoolapValue vals[16];
  read_num_args(env, argv, 1, len, vals);

  StoolapRows* rows = NULL;
  int32_t rc = S.stmt_query(cs->stmt, vals, (int32_t)len, &rows);

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_result_cached(env, rows, cs);
}

/* stmtQueryOneNums(external, num1, num2, ...): Object | null
 * Direct numeric args — no array unwrapping overhead. */
static napi_value wrap_stmt_query_one_nums(napi_env env, napi_callback_info info) {
  size_t argc = 17;
  napi_value argv[17];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  uint32_t len = (uint32_t)(argc - 1);
  if (len > 16) len = 16;

  StoolapValue vals[16];
  read_num_args(env, argv, 1, len, vals);

  StoolapRows* rows = NULL;
  int32_t rc = S.stmt_query(cs->stmt, vals, (int32_t)len, &rows);

  if (rc != S_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }

  return fetch_one_direct_cached(env, rows, cs);
}

/* stmtSql(external): string */
static napi_value wrap_stmt_sql(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  const char* sql = S.stmt_sql(cs->stmt);
  napi_value val;
  napi_create_string_utf8(env, sql ? sql : "", NAPI_AUTO_LENGTH, &val);
  return val;
}

/* stmtFinalize(external): void */
static napi_value wrap_stmt_finalize(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  if (cs->stmt) {
    S.stmt_finalize(cs->stmt);
    cs->stmt = NULL;  /* prevent double-free from GC destructor */
  }

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* txBegin(external): external */
static napi_value fn_tx_begin(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  StoolapTx* tx = NULL;
  int32_t rc = S.begin(db, &tx);
  if (rc != S_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Begin error");
  }

  napi_value ext;
  napi_create_external(env, tx, NULL, NULL, &ext);
  return ext;
}

/* txBeginAsync(external): Promise<external> */
static napi_value fn_tx_begin_async(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncTxBeginTask* task = calloc(1, sizeof(AsyncTxBeginTask));
  if (!task) THROW(env, "Out of memory");
  napi_get_value_external(env, argv[0], (void**)&task->db);

  napi_value promise = queue_async_work(
    env, task, &task->base, "txBeginAsync", execute_tx_begin_async, complete_tx_begin_async);
  if (!promise) {
    free(task);
    THROW(env, "Failed to queue txBeginAsync");
  }
  return promise;
}

/* txExec(external, sql, params?): { changes } */
static napi_value wrap_tx_exec(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  GET_SQL_STRING(env, argv[1], sql);

  int64_t affected = 0;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.tx_exec(tx, sql, &affected);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.tx_exec(tx, sql, &affected);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.tx_exec_p(tx, sql, vals, (int32_t)len, &affected);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.tx_exec_p(tx, sql, vals, cnt, &affected);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.tx_exec(tx, sql, &affected);
  }

  if (rc != S_OK) {
    const char* msg = S.tx_errmsg(tx);
    THROW(env, msg ? msg : "Transaction exec error");
  }

  return make_changes(env, affected);
}

/* txExecAsync(external, sql, params?): Promise<{ changes }> */
static napi_value wrap_tx_exec_async(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncExecTask* task = calloc(1, sizeof(AsyncExecTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_EXEC_TX;
  napi_get_value_external(env, argv[0], &task->target);
  if (copy_napi_string(env, argv[1], &task->sql) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  if (argc > 2) {
    if (convert_params_copy(env, argv[2], &task->vals, &task->val_count,
                            &task->heap_bufs, &task->heap_buf_count) != 0) {
      free(task->sql);
      free(task);
      THROW(env, "Failed to convert parameters");
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "txExecAsync", execute_async_exec, complete_async_exec);
  if (!promise) {
    free(task->sql);
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue txExecAsync");
  }
  return promise;
}

/* txQueryBuf(external, sql, params?): ArrayBuffer | null */
static napi_value wrap_tx_query_buf(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.tx_query(tx, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.tx_query(tx, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.tx_query_p(tx, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.tx_query_p(tx, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.tx_query(tx, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.tx_errmsg(tx);
    THROW(env, msg ? msg : "Transaction query error");
  }

  return fetch_as_arraybuffer(env, rows);
}

/* txQueryBufAsync(external, sql, params?): Promise<ArrayBuffer | null> */
static napi_value wrap_tx_query_buf_async(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncQueryTask* task = calloc(1, sizeof(AsyncQueryTask));
  if (!task) THROW(env, "Out of memory");

  task->kind = ASYNC_QUERY_TX;
  napi_get_value_external(env, argv[0], &task->target);
  if (copy_napi_string(env, argv[1], &task->sql) != 0) {
    free(task);
    THROW(env, "Out of memory");
  }

  if (argc > 2) {
    if (convert_params_copy(env, argv[2], &task->vals, &task->val_count,
                            &task->heap_bufs, &task->heap_buf_count) != 0) {
      free(task->sql);
      free(task);
      THROW(env, "Failed to convert parameters");
    }
  }

  napi_value promise = queue_async_work(
    env, task, &task->base, "txQueryBufAsync", execute_async_query, complete_async_query);
  if (!promise) {
    free(task->sql);
    free_params(task->vals, task->heap_bufs, task->heap_buf_count);
    free(task);
    THROW(env, "Failed to queue txQueryBufAsync");
  }
  return promise;
}

/* txQuery(external, sql, params?): Array | ArrayBuffer */
static napi_value wrap_tx_query(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.tx_query(tx, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.tx_query(tx, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.tx_query_p(tx, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.tx_query_p(tx, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.tx_query(tx, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.tx_errmsg(tx);
    THROW(env, msg ? msg : "Transaction query error");
  }

  return fetch_result(env, rows);
}

/* txQueryOne(external, sql, params?): Object | null */
static napi_value wrap_tx_query_one(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapRows* rows = NULL;
  int32_t rc;

  if (argc > 2) {
    napi_valuetype vt;
    napi_typeof(env, argv[2], &vt);
    if (vt == napi_undefined || vt == napi_null) {
      rc = S.tx_query(tx, sql, &rows);
    } else {
      uint32_t len;
      napi_get_array_length(env, argv[2], &len);
      if (len == 0) {
        rc = S.tx_query(tx, sql, &rows);
      } else if (len <= MAX_STACK_PARAMS) {
        StoolapValue vals[MAX_STACK_PARAMS];
        char* tbufs[MAX_STACK_PARAMS * 2];
        int tbuf_cnt = 0;
        for (uint32_t i = 0; i < len; i++) {
          napi_value elem;
          napi_get_element(env, argv[2], i, &elem);
          if (js_to_value(env, elem, &vals[i], tbufs, &tbuf_cnt) != 0) {
            for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
            THROW(env, "Failed to convert parameters");
          }
        }
        rc = S.tx_query_p(tx, sql, vals, (int32_t)len, &rows);
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
      } else {
        StoolapValue* vals; int32_t cnt;
        char** tbufs; int tbuf_cnt;
        if (convert_params(env, argv[2], &vals, &cnt, &tbufs, &tbuf_cnt) != 0)
          THROW(env, "Failed to convert parameters");
        rc = S.tx_query_p(tx, sql, vals, cnt, &rows);
        free_params(vals, tbufs, tbuf_cnt);
      }
    }
  } else {
    rc = S.tx_query(tx, sql, &rows);
  }

  if (rc != S_OK) {
    const char* msg = S.tx_errmsg(tx);
    THROW(env, msg ? msg : "Transaction query error");
  }

  return fetch_one(env, rows);
}

/* txCommit(external): void */
static napi_value wrap_tx_commit(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  int32_t rc = S.tx_commit(tx);
  if (rc != S_OK) THROW(env, "Failed to commit transaction");

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* txCommitAsync(external): Promise<void> */
static napi_value wrap_tx_commit_async(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncTxFinishTask* task = calloc(1, sizeof(AsyncTxFinishTask));
  if (!task) THROW(env, "Out of memory");
  task->kind = ASYNC_TX_COMMIT;
  napi_get_value_external(env, argv[0], (void**)&task->tx);

  napi_value promise = queue_async_work(
    env, task, &task->base, "txCommitAsync", execute_tx_finish_async, complete_tx_finish_async);
  if (!promise) {
    free(task);
    THROW(env, "Failed to queue txCommitAsync");
  }
  return promise;
}

/* txRollback(external): void */
static napi_value wrap_tx_rollback(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  int32_t rc = S.tx_rollback(tx);
  if (rc != S_OK) THROW(env, "Failed to rollback transaction");

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* txRollbackAsync(external): Promise<void> */
static napi_value wrap_tx_rollback_async(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  AsyncTxFinishTask* task = calloc(1, sizeof(AsyncTxFinishTask));
  if (!task) THROW(env, "Out of memory");
  task->kind = ASYNC_TX_ROLLBACK;
  napi_get_value_external(env, argv[0], (void**)&task->tx);

  napi_value promise = queue_async_work(
    env, task, &task->base, "txRollbackAsync", execute_tx_finish_async, complete_tx_finish_async);
  if (!promise) {
    free(task);
    THROW(env, "Failed to queue txRollbackAsync");
  }
  return promise;
}

/* txExecBatch(external, sql, paramsArrayOfArrays): { changes }
 * Batch exec in C — copies SQL once, pre-allocates param buffers,
 * loops param sets in C. Eliminates N × JS↔C crossings + N×malloc/free. */
static napi_value wrap_tx_exec_batch(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  GET_SQL_STRING(env, argv[1], sql);

  uint32_t batch_len;
  napi_get_array_length(env, argv[2], &batch_len);
  if (batch_len == 0) return make_changes(env, 0);

  /* Get param count from first element — pre-allocate ONCE */
  napi_value first_arr;
  napi_get_element(env, argv[2], 0, &first_arr);
  uint32_t param_count;
  napi_get_array_length(env, first_arr, &param_count);

  StoolapValue* vals = calloc(param_count, sizeof(StoolapValue));
  char** tbufs = calloc(param_count * 2, sizeof(char*));

  int64_t total_affected = 0;

  for (uint32_t i = 0; i < batch_len; i++) {
    napi_value params_arr;
    napi_get_element(env, argv[2], i, &params_arr);

    /* Convert params into pre-allocated buffers */
    int tbuf_cnt = 0;
    for (uint32_t j = 0; j < param_count; j++) {
      napi_value elem;
      napi_get_element(env, params_arr, j, &elem);
      if (js_to_value(env, elem, &vals[j], tbufs, &tbuf_cnt) != 0) {
        /* Cleanup on error */
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
        free(vals); free(tbufs);
        THROW(env, "Failed to convert parameters");
      }
    }

    int64_t affected = 0;
    int32_t rc = S.tx_exec_p(tx, sql, vals, (int32_t)param_count, &affected);

    /* Free only text buffers, keep vals/tbufs arrays for reuse */
    for (int k = 0; k < tbuf_cnt; k++) { free(tbufs[k]); tbufs[k] = NULL; }

    if (rc != S_OK) {
      free(vals); free(tbufs);
      const char* msg = S.tx_errmsg(tx);
      THROW(env, msg ? msg : "Transaction batch exec error");
    }

    total_affected += affected;
  }

  free(vals);
  free(tbufs);
  return make_changes(env, total_affected);
}

/* stmtExecBatch(external, paramsArrayOfArrays): { changes }
 * Batch exec via prepared statement — pre-allocated buffers, loops in C. */
static napi_value wrap_stmt_exec_batch(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  uint32_t batch_len;
  napi_get_array_length(env, argv[1], &batch_len);
  if (batch_len == 0) return make_changes(env, 0);

  /* Get param count from first element — pre-allocate ONCE */
  napi_value first_arr;
  napi_get_element(env, argv[1], 0, &first_arr);
  uint32_t param_count;
  napi_get_array_length(env, first_arr, &param_count);

  StoolapValue* vals = calloc(param_count, sizeof(StoolapValue));
  char** tbufs = calloc(param_count * 2, sizeof(char*));

  int64_t total_affected = 0;

  for (uint32_t i = 0; i < batch_len; i++) {
    napi_value params_arr;
    napi_get_element(env, argv[1], i, &params_arr);

    int tbuf_cnt = 0;
    for (uint32_t j = 0; j < param_count; j++) {
      napi_value elem;
      napi_get_element(env, params_arr, j, &elem);
      if (js_to_value(env, elem, &vals[j], tbufs, &tbuf_cnt) != 0) {
        for (int k = 0; k < tbuf_cnt; k++) free(tbufs[k]);
        free(vals); free(tbufs);
        THROW(env, "Failed to convert parameters");
      }
    }

    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, vals, (int32_t)param_count, &affected);

    for (int k = 0; k < tbuf_cnt; k++) { free(tbufs[k]); tbufs[k] = NULL; }

    if (rc != S_OK) {
      free(vals); free(tbufs);
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement batch exec error");
    }

    total_affected += affected;
  }

  free(vals);
  free(tbufs);
  return make_changes(env, total_affected);
}

/* ---- Binary param buffer protocol ---- */
/* JS encodes params to a Buffer, C reads directly — eliminates per-param N-API calls.
 * Format per param: [uint8 tag][value bytes]
 *   0=NULL, 1=INT32(4B), 2=F64(8B), 3=TEXT(u32 len + data + \0), 4=BOOL(1B), 5=INT64(8B) */

#define PBUF_NULL  0
#define PBUF_I32   1
#define PBUF_F64   2
#define PBUF_TEXT  3
#define PBUF_BOOL  4
#define PBUF_I64   5

static int decode_params_buf(const uint8_t* buf, size_t buf_len,
                             StoolapValue* vals, int max_params,
                             size_t* consumed) {
  const uint8_t* ptr = buf;
  const uint8_t* end = buf + buf_len;
  int count = 0;
  while (ptr < end && count < max_params) {
    uint8_t tag = *ptr++;
    switch (tag) {
      case PBUF_NULL:
        vals[count].value_type = T_NULL;
        vals[count]._padding = 0;
        break;
      case PBUF_I32: {
        if (ptr + 4 > end) return -1;
        int32_t i32; memcpy(&i32, ptr, 4); ptr += 4;
        vals[count].value_type = T_INTEGER;
        vals[count]._padding = 0;
        vals[count].v.integer = (int64_t)i32;
        break;
      }
      case PBUF_F64: {
        if (ptr + 8 > end) return -1;
        double d; memcpy(&d, ptr, 8); ptr += 8;
        int64_t i64 = (int64_t)d;
        if ((double)i64 == d && d >= -9007199254740992.0 && d <= 9007199254740992.0) {
          vals[count].value_type = T_INTEGER;
          vals[count]._padding = 0;
          vals[count].v.integer = i64;
        } else {
          vals[count].value_type = T_FLOAT;
          vals[count]._padding = 0;
          vals[count].v.float64 = d;
        }
        break;
      }
      case PBUF_TEXT: {
        if (ptr + 4 > end) return -1;
        uint32_t len; memcpy(&len, ptr, 4); ptr += 4;
        if (ptr + len + 1 > end) return -1;
        vals[count].value_type = T_TEXT;
        vals[count]._padding = 0;
        vals[count].v.text.ptr = (const char*)ptr;
        vals[count].v.text.len = (int64_t)len;
        ptr += len + 1; /* skip null terminator */
        break;
      }
      case PBUF_BOOL:
        if (ptr >= end) return -1;
        vals[count].value_type = T_BOOLEAN;
        vals[count]._padding = 0;
        vals[count].v.boolean = *ptr++;
        break;
      case PBUF_I64: {
        if (ptr + 8 > end) return -1;
        int64_t i64; memcpy(&i64, ptr, 8); ptr += 8;
        vals[count].value_type = T_INTEGER;
        vals[count]._padding = 0;
        vals[count].v.integer = i64;
        break;
      }
      default: return -1;
    }
    count++;
  }
  if (consumed) *consumed = (size_t)(ptr - buf);
  return count;
}

/* dbExecBatchBuf(db, sql, buffer) → { changes }
 * Batch format: [u32 row_count][u8 params_per_row][row0..rowN params] */
static napi_value wrap_db_exec_batch_buf(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  GET_SQL_STRING(env, argv[1], sql);
  void* buf_data; size_t buf_len;
  napi_get_buffer_info(env, argv[2], &buf_data, &buf_len);
  const uint8_t* ptr = (const uint8_t*)buf_data;
  const uint8_t* end = ptr + buf_len;
  if (ptr + 5 > end) THROW(env, "Batch buffer too short");
  uint32_t row_count; memcpy(&row_count, ptr, 4); ptr += 4;
  uint8_t ppr = *ptr++;
  if (row_count == 0) return make_changes(env, 0);
  StoolapTx* tx = NULL;
  int32_t rc = S.begin(db, &tx);
  if (rc != S_OK) { const char* msg = S.errmsg(db); THROW(env, msg ? msg : "Begin error"); }
  StoolapStmt* stmt = NULL;
  rc = S.prepare(db, sql, &stmt);
  if (rc != S_OK) { S.tx_rollback(tx); const char* msg = S.errmsg(db); THROW(env, msg ? msg : "Prepare error"); }
  int64_t total = 0;
  for (uint32_t i = 0; i < row_count; i++) {
    StoolapValue vals[MAX_STACK_PARAMS];
    size_t consumed = 0;
    int decoded = decode_params_buf(ptr, (size_t)(end - ptr), vals, ppr, &consumed);
    if (decoded != ppr) { S.stmt_finalize(stmt); S.tx_rollback(tx); THROW(env, "Batch param decode error"); }
    ptr += consumed;
    int64_t affected = 0;
    rc = S.stmt_exec(stmt, vals, decoded, &affected);
    if (rc != S_OK) { const char* msg = S.stmt_errmsg(stmt); S.stmt_finalize(stmt); S.tx_rollback(tx); THROW(env, msg ? msg : "Batch exec error"); }
    total += affected;
  }
  S.stmt_finalize(stmt);
  rc = S.tx_commit(tx);
  if (rc != S_OK) { const char* msg = S.errmsg(db); THROW(env, msg ? msg : "Commit error"); }
  return make_changes(env, total);
}

/* stmtExecBatchBuf(external, buffer): { changes }
 * Binary batch protocol for prepared statements — eliminates per-param N-API calls. */
static napi_value wrap_stmt_exec_batch_buf(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  GET_STMT(env, argv[0], cs);
  StoolapStmt* stmt = cs->stmt;

  void* buf_data; size_t buf_len;
  napi_get_buffer_info(env, argv[1], &buf_data, &buf_len);
  const uint8_t* ptr = (const uint8_t*)buf_data;
  const uint8_t* end = ptr + buf_len;

  if (ptr + 5 > end) THROW(env, "Batch buffer too short");
  uint32_t row_count; memcpy(&row_count, ptr, 4); ptr += 4;
  uint8_t ppr = *ptr++;
  if (row_count == 0) return make_changes(env, 0);

  int64_t total = 0;
  for (uint32_t i = 0; i < row_count; i++) {
    StoolapValue vals[MAX_STACK_PARAMS];
    size_t consumed = 0;
    int decoded = decode_params_buf(ptr, (size_t)(end - ptr), vals, ppr, &consumed);
    if (decoded != ppr) THROW(env, "Batch param decode error");
    ptr += consumed;
    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, vals, decoded, &affected);
    if (rc != S_OK) {
      const char* msg = S.stmt_errmsg(stmt);
      THROW(env, msg ? msg : "Statement batch exec error");
    }
    total += affected;
  }
  return make_changes(env, total);
}

/* ============================================================
 * Module initialization
 * ============================================================ */

#define EXPORT_FN(env, exports, name, fn) do { \
  napi_value _fn; \
  napi_create_function(env, name, NAPI_AUTO_LENGTH, fn, NULL, &_fn); \
  napi_set_named_property(env, exports, name, _fn); \
} while(0)

static napi_value Init(napi_env env, napi_value exports) {
  EXPORT_FN(env, exports, "loadLibrary",   fn_load_library);
  EXPORT_FN(env, exports, "dbOpen",        fn_db_open);
  EXPORT_FN(env, exports, "dbOpenAsync",   fn_db_open_async);
  EXPORT_FN(env, exports, "dbClose",       fn_db_close);
  EXPORT_FN(env, exports, "dbCloseAsync",  fn_db_close_async);
  EXPORT_FN(env, exports, "dbExec",        fn_db_exec);
  EXPORT_FN(env, exports, "dbExecAsync",   fn_db_exec_async);
  EXPORT_FN(env, exports, "dbExecSimple",  fn_db_exec_simple);
  EXPORT_FN(env, exports, "dbExecSimpleAsync", fn_db_exec_simple_async);
  EXPORT_FN(env, exports, "dbQueryBuf",    fn_db_query_buf);
  EXPORT_FN(env, exports, "dbQueryBufAsync", fn_db_query_buf_async);
  EXPORT_FN(env, exports, "dbQuery",       fn_db_query);
  EXPORT_FN(env, exports, "dbQueryOne",    fn_db_query_one);
  EXPORT_FN(env, exports, "dbPrepare",     fn_db_prepare);
  EXPORT_FN(env, exports, "stmtExec",      wrap_stmt_exec);
  EXPORT_FN(env, exports, "stmtExecAsync", wrap_stmt_exec_async);
  EXPORT_FN(env, exports, "stmtQueryBuf",  wrap_stmt_query_buf);
  EXPORT_FN(env, exports, "stmtQueryBufAsync", wrap_stmt_query_buf_async);
  EXPORT_FN(env, exports, "stmtQuery",     wrap_stmt_query);
  EXPORT_FN(env, exports, "stmtQueryOne",  wrap_stmt_query_one);
  EXPORT_FN(env, exports, "stmtSql",       wrap_stmt_sql);
  EXPORT_FN(env, exports, "stmtExecNums",     wrap_stmt_exec_nums);
  EXPORT_FN(env, exports, "stmtQueryNums",    wrap_stmt_query_nums);
  EXPORT_FN(env, exports, "stmtQueryOneNums", wrap_stmt_query_one_nums);
  EXPORT_FN(env, exports, "stmtExecBatch",    wrap_stmt_exec_batch);
  EXPORT_FN(env, exports, "stmtExecBatchBuf", wrap_stmt_exec_batch_buf);
  EXPORT_FN(env, exports, "stmtFinalize",  wrap_stmt_finalize);
  EXPORT_FN(env, exports, "dbExecBatchBuf",   wrap_db_exec_batch_buf);
  EXPORT_FN(env, exports, "txBegin",       fn_tx_begin);
  EXPORT_FN(env, exports, "txBeginAsync",  fn_tx_begin_async);
  EXPORT_FN(env, exports, "txExec",        wrap_tx_exec);
  EXPORT_FN(env, exports, "txExecAsync",   wrap_tx_exec_async);
  EXPORT_FN(env, exports, "txExecBatch",   wrap_tx_exec_batch);
  EXPORT_FN(env, exports, "txQueryBuf",    wrap_tx_query_buf);
  EXPORT_FN(env, exports, "txQueryBufAsync", wrap_tx_query_buf_async);
  EXPORT_FN(env, exports, "txQuery",       wrap_tx_query);
  EXPORT_FN(env, exports, "txQueryOne",    wrap_tx_query_one);
  EXPORT_FN(env, exports, "txCommit",      wrap_tx_commit);
  EXPORT_FN(env, exports, "txCommitAsync", wrap_tx_commit_async);
  EXPORT_FN(env, exports, "txRollback",    wrap_tx_rollback);
  EXPORT_FN(env, exports, "txRollbackAsync", wrap_tx_rollback_async);
  return exports;
}

NAPI_MODULE(stoolap, Init)
