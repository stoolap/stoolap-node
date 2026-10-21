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
#include <stdio.h>

#ifdef _WIN32
  #include <windows.h>
  #include <malloc.h>
  #define DLOPEN(path)       LoadLibraryA(path)
  #define DLSYM(lib, name)   GetProcAddress(lib, name)
  #define DLERROR()          "LoadLibrary failed"
  #define alloca             _alloca
  typedef HMODULE LibHandle;
#else
  #include <dlfcn.h>
  #include <alloca.h>
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

#define STOOLAP_OK    0
#define STOOLAP_ERR   1
#define STOOLAP_ROW   100
#define STOOLAP_DONE  101

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

/* Named parameter (:name) — name is not NUL-terminated, length in name_len */
typedef struct {
  const char* name;
  int32_t name_len;
  int32_t _padding;
  StoolapValue value;
} StoolapNamedParam;

/* ---- Function pointer types ---- */

typedef const char* (*fn_version)(void);
typedef int32_t (*fn_open)(const char*, StoolapDB**);
typedef int32_t (*fn_open_mem)(StoolapDB**);
typedef int32_t (*fn_clone)(StoolapDB*, StoolapDB**);
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
typedef int32_t (*fn_tx_stmt_exec)(StoolapTx*, StoolapStmt*, const StoolapValue*, int32_t, int64_t*);
typedef int32_t (*fn_tx_commit)(StoolapTx*);
typedef int32_t (*fn_tx_rollback)(StoolapTx*);
typedef const char* (*fn_tx_errmsg)(const StoolapTx*);
typedef void (*fn_rows_close)(StoolapRows*);
typedef int32_t (*fn_rows_fetch_all)(StoolapRows*, uint8_t**, int64_t*);
typedef void (*fn_buffer_free)(uint8_t*, int64_t);
typedef const char* (*fn_rows_errmsg)(const StoolapRows*);
typedef int32_t (*fn_rows_fetch_chunk)(StoolapRows*, int32_t, const uint8_t**, int64_t*, int32_t*);
typedef int32_t (*fn_exec_named)(StoolapDB*, const char*, const StoolapNamedParam*, int32_t, int64_t*);
typedef int32_t (*fn_query_named)(StoolapDB*, const char*, const StoolapNamedParam*, int32_t, StoolapRows**);
typedef int32_t (*fn_tx_exec_named)(StoolapTx*, const char*, const StoolapNamedParam*, int32_t, int64_t*);
typedef int32_t (*fn_tx_query_named)(StoolapTx*, const char*, const StoolapNamedParam*, int32_t, StoolapRows**);
typedef int32_t (*fn_stmt_query_all)(StoolapStmt*, StoolapTx*, const StoolapValue*, int32_t, const uint8_t**, int64_t*);
typedef int32_t (*fn_query_params_all)(StoolapDB*, StoolapTx*, const char*, const StoolapValue*, int32_t, const uint8_t**, int64_t*);
typedef int32_t (*fn_query_named_all)(StoolapDB*, StoolapTx*, const char*, const StoolapNamedParam*, int32_t, const uint8_t**, int64_t*);

/* ---- Loaded function pointers ---- */

static struct {
  fn_version version;
  fn_open open;
  fn_open_mem open_mem;
  fn_clone clone;
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
  fn_tx_stmt_exec tx_stmt_exec;
  fn_tx_commit tx_commit;
  fn_tx_rollback tx_rollback;
  fn_tx_errmsg tx_errmsg;
  fn_rows_close rows_close;
  fn_rows_fetch_all rows_fetch_all;
  fn_buffer_free buffer_free;
  fn_rows_errmsg rows_errmsg;
  fn_rows_fetch_chunk rows_fetch_chunk;
  fn_exec_named exec_named;
  fn_query_named query_named;
  fn_tx_exec_named tx_exec_named;
  fn_tx_query_named tx_query_named;
  fn_stmt_query_all stmt_query_all;
  fn_query_params_all query_params_all;
  fn_query_named_all query_named_all;
} S;

static LibHandle lib_handle = NULL;

/* ---- Helpers ---- */

#define THROW(env, msg) do { napi_throw_error(env, NULL, msg); return NULL; } while(0)
#define ERR_SAVE(msg, fallback) \
  char _eb[512]; snprintf(_eb, sizeof(_eb), "%s", (msg) ? (msg) : (fallback))

/* True when d is an integer the engine can hold exactly; false for NaN,
 * infinities and anything outside 2^53 (casting those is undefined). */
static inline int is_safe_integer(double d) {
  return d == d && d >= -9007199254740992.0 && d <= 9007199254740992.0 && (double)(int64_t)d == d;
}

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

/* ---- Text arena: strings bound for one call live on the stack; only strings
 * that do not fit are malloc'd and freed with the arena. ---- */
/* A string copied with N-API "fits" only when there was room for one more
 * UTF-8 sequence: V8 never splits a character, so a copy short of the
 * buffer by less than that may still be truncated. */
#define UTF8_SLACK 4

#define PARAM_ARENA_SIZE 2048
typedef struct {
  char buf[PARAM_ARENA_SIZE];
  size_t used;
  char** heap;
  int heap_count, heap_cap;
} TextArena;
#define ARENA_INIT(a) do { (a).used = 0; (a).heap = NULL; (a).heap_count = 0; (a).heap_cap = 0; } while (0)

static char* arena_alloc(TextArena* a, size_t len) {
  if (a->used + len <= PARAM_ARENA_SIZE) {
    char* p = a->buf + a->used;
    a->used += len;
    return p;
  }
  char* p = malloc(len);
  if (!p) return NULL;
  if (a->heap_count == a->heap_cap) {
    int cap = a->heap_cap ? a->heap_cap * 2 : 8;
    char** grown = realloc(a->heap, (size_t)cap * sizeof(char*));
    if (!grown) { free(p); return NULL; }
    a->heap = grown;
    a->heap_cap = cap;
  }
  a->heap[a->heap_count++] = p;
  return p;
}

static void arena_reset(TextArena* a) {
  for (int i = 0; i < a->heap_count; i++) free(a->heap[i]);
  free(a->heap);
  ARENA_INIT(*a);
}

/* Copy a JS string into the arena, NUL-terminated. Writes straight into the
 * arena's free space when it fits, so a short string is copied once. */
static char* arena_string(napi_env env, napi_value jsval, TextArena* a, size_t* out_len) {
  size_t avail = PARAM_ARENA_SIZE - a->used;
  size_t len = 0;
  if (avail > UTF8_SLACK) {
    char* dst = a->buf + a->used;
    if (napi_get_value_string_utf8(env, jsval, dst, avail, &len) != napi_ok) return NULL;
    if (len + UTF8_SLACK < avail) {
      a->used += len + 1;
      *out_len = len;
      return dst;
    }
  }
  if (napi_get_value_string_utf8(env, jsval, NULL, 0, &len) != napi_ok) return NULL;
  char* dst = arena_alloc(a, len + 1);
  if (!dst) return NULL;
  napi_get_value_string_utf8(env, jsval, dst, len + 1, &len);
  *out_len = len;
  return dst;
}

/* Convert a JS value to a StoolapValue; text is copied into `ta`. */
static int js_to_value(napi_env env, napi_value jsval, StoolapValue* out, TextArena* ta) {
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
    if (is_safe_integer(d)) {
      int64_t i64 = (int64_t)d;
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
    if (!lossless) return -1; /* outside int64 */
    out->value_type = T_INTEGER;
    out->_padding = 0;
    out->v.integer = i64;
    return 0;
  }

  if (vt == napi_string) {
    size_t len;
    char* buf = arena_string(env, jsval, ta, &len);
    if (!buf) return -1;
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
      if (ms != ms) { out->value_type = T_NULL; out->_padding = 0; out->v.integer = 0; return 0; }
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
      char* buf = arena_alloc(ta, len + 1);
      if (!buf) return -1;
      memcpy(buf, data, len);
      buf[len] = 0;
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
    char* buf = arena_string(env, json_str, ta, &len);
    if (!buf) return -1;
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

#define MAX_STACK_PARAMS 16

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
    if (is_safe_integer(d)) {
      int64_t i64 = (int64_t)d;
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
    if (!lossless) return -1; /* outside int64 */
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
      if (ms != ms) { out->value_type = T_NULL; out->_padding = 0; out->v.integer = 0; return 0; }
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
#define DIRECT_ROW_LIMIT 16

/* ---- Cached prepared statement wrapper ---- */

typedef struct {
  StoolapStmt* stmt;
  int32_t col_count;
  uint32_t header_size;  /* cached byte offset of row_count in the packed buffer */
  char col_names[MAX_DIRECT_COLS][128];
  int has_cache;
  /* the packed header the cache was built from, compared on every call so a
   * schema change after prepare is noticed */
  uint8_t* header;
  /* JS row factory for this column list (built by the maker in lib/parse.js):
   * one napi_call_function builds a row with a stable hidden class */
  napi_ref factory;
} CachedStmt;

/* makeRowFactory from lib/parse.js, handed over at load time */
static napi_ref row_factory_maker = NULL;

static void finalize_cached_stmt(napi_env env, void* data, void* hint) {
  (void)env; (void)hint;
  CachedStmt* cs = (CachedStmt*)data;
  if (cs) {
    if (cs->stmt) S.stmt_finalize(cs->stmt);
    if (cs->factory) napi_delete_reference(env, cs->factory);
    free(cs->header);
    free(cs);
  }
}

#define GET_STMT(env, argv_0, cs_var) \
  CachedStmt* cs_var; \
  napi_get_value_external(env, argv_0, (void**)&cs_var)

/* ---- SQL string helper: single-copy for short strings ---- */

#define SQL_STACK_SIZE 512

/* SQL longer than the stack buffer goes to a scratch buffer that is reused
 * across calls. Only the synchronous wrappers use it, so it needs no lock. */
static char* sql_scratch = NULL;
static size_t sql_scratch_cap = 0;

static char* sql_scratch_get(size_t len) {
  if (len + 1 > sql_scratch_cap) {
    size_t cap = len + 1 + len / 2;
    char* grown = realloc(sql_scratch, cap);
    if (!grown) return NULL;
    sql_scratch = grown;
    sql_scratch_cap = cap;
  }
  return sql_scratch;
}

#define GET_SQL_STRING(env, jsval, sql_var) \
  char _stack_sql[SQL_STACK_SIZE]; \
  size_t _sql_len = 0; \
  char* sql_var; \
  if (napi_get_value_string_utf8(env, jsval, _stack_sql, SQL_STACK_SIZE, &_sql_len) != napi_ok) { \
    napi_throw_type_error(env, NULL, "sql must be a string"); \
    return NULL; \
  } \
  if (_sql_len + UTF8_SLACK < SQL_STACK_SIZE) { \
    sql_var = _stack_sql; \
  } else { \
    napi_get_value_string_utf8(env, jsval, NULL, 0, &_sql_len); \
    sql_var = sql_scratch_get(_sql_len); \
    if (!sql_var) THROW(env, "Out of memory"); \
    napi_get_value_string_utf8(env, jsval, sql_var, _sql_len + 1, &_sql_len); \
  }

/* ---- Parameter binding ---- */
typedef struct {
  StoolapValue stack_vals[MAX_STACK_PARAMS];
  StoolapValue* vals;
  int32_t count;
  TextArena ta;
} ParamBind;

static void bind_release(ParamBind* pb) {
  arena_reset(&pb->ta);
  if (pb->vals != pb->stack_vals) free(pb->vals);
  pb->vals = pb->stack_vals;
  pb->count = 0;
}

/* Bind a JS array of positional params (undefined/null = none).
 * Returns 0, or -1 after throwing. */
static int bind_positional(napi_env env, napi_value arr, ParamBind* pb) {
  pb->vals = pb->stack_vals;
  pb->count = 0;
  ARENA_INIT(pb->ta);
  napi_valuetype vt;
  napi_typeof(env, arr, &vt);
  if (vt == napi_undefined || vt == napi_null) return 0;
  uint32_t len = 0;
  if (napi_get_array_length(env, arr, &len) != napi_ok) {
    napi_throw_type_error(env, NULL, "params must be an array");
    return -1;
  }
  if (len == 0) return 0;
  if (len > MAX_STACK_PARAMS) {
    pb->vals = malloc(len * sizeof(StoolapValue));
    if (!pb->vals) { pb->vals = pb->stack_vals; napi_throw_error(env, NULL, "Out of memory"); return -1; }
  }
  for (uint32_t i = 0; i < len; i++) {
    napi_value elem;
    napi_get_element(env, arr, i, &elem);
    if (js_to_value(env, elem, &pb->vals[i], &pb->ta) != 0) {
      bind_release(pb);
      napi_throw_error(env, NULL, "Failed to convert parameters");
      return -1;
    }
  }
  pb->count = (int32_t)len;
  return 0;
}

typedef struct {
  StoolapNamedParam stack_np[MAX_STACK_PARAMS];
  StoolapNamedParam* np;
  int32_t count;
  TextArena ta;
} NamedBind;

static void named_release(NamedBind* nb) {
  arena_reset(&nb->ta);
  if (nb->np != nb->stack_np) free(nb->np);
  nb->np = nb->stack_np;
  nb->count = 0;
}

/* Bind named params from parallel JS arrays: `names` (strings) and `values`.
 * Returns 0, or -1 after throwing. */
static int bind_named(napi_env env, napi_value names, napi_value values, NamedBind* nb) {
  nb->np = nb->stack_np;
  nb->count = 0;
  ARENA_INIT(nb->ta);
  uint32_t len = 0;
  if (napi_get_array_length(env, names, &len) != napi_ok) {
    napi_throw_type_error(env, NULL, "params must be an object");
    return -1;
  }
  if (len == 0) return 0;
  if (len > MAX_STACK_PARAMS) {
    nb->np = malloc(len * sizeof(StoolapNamedParam));
    if (!nb->np) { nb->np = nb->stack_np; napi_throw_error(env, NULL, "Out of memory"); return -1; }
  }
  for (uint32_t i = 0; i < len; i++) {
    StoolapNamedParam* np = &nb->np[i];
    np->_padding = 0;
    napi_value n;
    napi_get_element(env, names, i, &n);
    size_t nlen;
    char* name = arena_string(env, n, &nb->ta, &nlen);
    if (!name) { named_release(nb); napi_throw_error(env, NULL, "Out of memory"); return -1; }
    np->name = name;
    np->name_len = (int32_t)nlen;
    napi_value v;
    napi_get_element(env, values, i, &v);
    if (js_to_value(env, v, &np->value, &nb->ta) != 0) {
      named_release(nb);
      napi_throw_error(env, NULL, "Failed to convert parameters");
      return -1;
    }
  }
  nb->count = (int32_t)len;
  return 0;
}

/* Optional transaction argument: external or undefined/null */
static StoolapTx* tx_arg(napi_env env, napi_value v) {
  napi_valuetype vt;
  napi_typeof(env, v, &vt);
  if (vt == napi_undefined || vt == napi_null) return NULL;
  StoolapTx* tx = NULL;
  napi_get_value_external(env, v, (void**)&tx);
  return tx;
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

static napi_value read_buf_val(napi_env env, const uint8_t* buf, uint32_t* poff);

/* Copy a handle-owned packed buffer into a fresh ArrayBuffer */
static napi_value arraybuffer_copy(napi_env env, const uint8_t* buf, int64_t len) {
  void* data;
  napi_value ab;
  napi_create_arraybuffer(env, (size_t)len, &data, &ab);
  if (len > 0) memcpy(data, buf, (size_t)len);
  return ab;
}

/* Build one row object from the packed buffer at *off (advanced past the row).
 * napi_define_properties sets every column in one N-API call. */
static napi_value packed_row(napi_env env, const uint8_t* buf, uint32_t* off,
                             char names[][128], int32_t col_count) {
  napi_value row_obj;
  napi_create_object(env, &row_obj);
  napi_property_descriptor props[MAX_DIRECT_COLS];
  for (int32_t c = 0; c < col_count; c++) {
    props[c] = (napi_property_descriptor){
      .utf8name = names[c],
      .name = NULL, .method = NULL, .getter = NULL, .setter = NULL,
      .value = read_buf_val(env, buf, off),
      .attributes = napi_default_jsproperty, .data = NULL,
    };
  }
  napi_define_properties(env, row_obj, (size_t)col_count, props);
  return row_obj;
}

/* Ask lib/parse.js for a row factory for these column names; NULL when it
 * declines (no columns, or more than it compiles for). */
static napi_value build_row_factory(napi_env env, char names[][128], int32_t col_count) {
  if (!row_factory_maker) return NULL;
  napi_value maker, names_arr, factory;
  napi_get_reference_value(env, row_factory_maker, &maker);
  napi_create_array_with_length(env, (size_t)col_count, &names_arr);
  for (int32_t i = 0; i < col_count; i++) {
    napi_value name;
    napi_create_string_utf8(env, names[i], NAPI_AUTO_LENGTH, &name);
    napi_set_element(env, names_arr, (uint32_t)i, name);
  }
  napi_valuetype ft = napi_undefined;
  if (napi_call_function(env, maker, maker, 1, &names_arr, &factory) != napi_ok ||
      napi_typeof(env, factory, &ft) != napi_ok || ft != napi_function) {
    return NULL;
  }
  return factory;
}

/* Row factories for ad hoc statements, keyed by the packed header bytes.
 * A handful of hot queries keep their factory; the rest round-robin. */
#define FACTORY_CACHE_SIZE 8
typedef struct {
  uint8_t* header;
  uint32_t len;
  napi_ref factory;
} FactoryEntry;
static FactoryEntry factory_cache[FACTORY_CACHE_SIZE];
static int factory_cache_next = 0;

static napi_value adhoc_row_factory(napi_env env, const uint8_t* buf, uint32_t header_len,
                                    char names[][128], int32_t col_count) {
  for (int i = 0; i < FACTORY_CACHE_SIZE; i++) {
    FactoryEntry* e = &factory_cache[i];
    if (e->factory && e->len == header_len && memcmp(e->header, buf, header_len) == 0) {
      napi_value f;
      napi_get_reference_value(env, e->factory, &f);
      return f;
    }
  }
  napi_value f = build_row_factory(env, names, col_count);
  if (!f) return NULL;
  uint8_t* copy = malloc(header_len);
  if (!copy) return f;
  memcpy(copy, buf, header_len);
  FactoryEntry* e = &factory_cache[factory_cache_next];
  factory_cache_next = (factory_cache_next + 1) % FACTORY_CACHE_SIZE;
  if (e->factory) napi_delete_reference(env, e->factory);
  free(e->header);
  e->header = copy;
  e->len = header_len;
  napi_create_reference(env, f, 1, &e->factory);
  return f;
}

/* Build one row by calling the statement's JS row factory with the values. */
static napi_value factory_row(napi_env env, const uint8_t* buf, uint32_t* off,
                              napi_value factory, int32_t col_count) {
  napi_value argv[MAX_DIRECT_COLS];
  for (int32_t c = 0; c < col_count; c++) argv[c] = read_buf_val(env, buf, off);
  napi_value row;
  if (napi_call_function(env, factory, factory, (size_t)col_count, argv, &row) != napi_ok) {
    napi_get_null(env, &row);
  }
  return row;
}

#define RESULT_ROWS 0  /* hybrid: objects for small results, ArrayBuffer for JS parsing otherwise */
#define RESULT_ONE  1  /* first row object, or null */
#define RESULT_RAW  2  /* ArrayBuffer */

/* Turn the packed result of a *_query_all call into the JS value for `mode`.
 * `cs` caches the column header of a prepared statement across calls.
 * Results with more than MAX_DIRECT_COLS columns always go to JS as an
 * ArrayBuffer; for RESULT_ONE the JS side then takes the first row. */
static napi_value packed_result(napi_env env, const uint8_t* buf, int64_t len,
                                int mode, CachedStmt* cs) {
  if (mode == RESULT_RAW) return arraybuffer_copy(env, buf, len);
  uint32_t cc;
  memcpy(&cc, buf, 4);
  if (cc > MAX_DIRECT_COLS) return arraybuffer_copy(env, buf, len);
  int32_t col_count = (int32_t)cc;
  char local_names[MAX_DIRECT_COLS][128];
  char (*names)[128];
  uint32_t off;
  if (cs && cs->has_cache && cs->col_count == col_count &&
      (int64_t)cs->header_size <= len && memcmp(buf, cs->header, cs->header_size) == 0) {
    names = cs->col_names;
    off = cs->header_size;
  } else {
    names = cs ? cs->col_names : local_names;
    off = 4;
    for (int32_t i = 0; i < col_count; i++) {
      uint16_t nlen;
      memcpy(&nlen, buf + off, 2); off += 2;
      /* a name longer than the C key buffer: let JS build the keys */
      if (nlen > 127) return arraybuffer_copy(env, buf, len);
      memcpy(names[i], buf + off, nlen);
      names[i][nlen] = '\0';
      off += nlen;
    }
    if (cs) {
      uint8_t* copy = realloc(cs->header, off);
      if (copy) {
        memcpy(copy, buf, off);
        cs->header = copy;
        cs->col_count = col_count;
        cs->header_size = off;
        cs->has_cache = 1;
      } else {
        cs->has_cache = 0;
      }
      if (cs->factory) {
        napi_delete_reference(env, cs->factory);
        cs->factory = NULL;
      }
      napi_value f = build_row_factory(env, names, col_count);
      if (f) napi_create_reference(env, f, 1, &cs->factory);
    }
  }
  uint32_t row_count;
  memcpy(&row_count, buf + off, 4); off += 4;
  napi_value factory = NULL;
  if (cs) {
    if (cs->factory) napi_get_reference_value(env, cs->factory, &factory);
  } else {
    factory = adhoc_row_factory(env, buf, off, names, col_count);
  }
  if (mode == RESULT_ONE) {
    if (row_count == 0) {
      napi_value null_val;
      napi_get_null(env, &null_val);
      return null_val;
    }
    return factory ? factory_row(env, buf, &off, factory, col_count)
                   : packed_row(env, buf, &off, names, col_count);
  }
  if (row_count > DIRECT_ROW_LIMIT) return arraybuffer_copy(env, buf, len);
  napi_value result;
  napi_create_array_with_length(env, row_count, &result);
  for (uint32_t r = 0; r < row_count; r++) {
    napi_value row = factory ? factory_row(env, buf, &off, factory, col_count)
                             : packed_row(env, buf, &off, names, col_count);
    napi_set_element(env, result, r, row);
  }
  return result;
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

/* Bound function: queryOne with a single integer param, CachedStmt baked in.
 * Saves the napi_get_value_external of the generic path. */
static napi_value wrap_stmt_query_one_int_bound(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  void* data;
  napi_get_cb_info(env, info, &argc, argv, NULL, &data);
  CachedStmt* cs = (CachedStmt*)data;
  double d;
  napi_get_value_double(env, argv[0], &d);
  StoolapValue val;
  val._padding = 0;
  if (is_safe_integer(d)) {
    val.value_type = T_INTEGER;
    val.v.integer = (int64_t)d;
  } else {
    val.value_type = T_FLOAT;
    val.v.float64 = d;
  }
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t rc = S.stmt_query_all(cs->stmt, NULL, &val, 1, &buf, &len);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }
  return packed_result(env, buf, len, RESULT_ONE, cs);
}

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

  if (rc != STOOLAP_OK) {
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
  int32_t rc = STOOLAP_ERR;

  switch (task->kind) {
    case ASYNC_EXEC_DB: {
      StoolapDB* db = (StoolapDB*)task->target;
      rc = task->val_count > 0
        ? S.exec_p(db, task->sql, task->vals, task->val_count, &task->affected)
        : S.exec(db, task->sql, &task->affected);
      if (rc != STOOLAP_OK) task->base.error = dup_cstr(S.errmsg(db), "Exec error");
      break;
    }
    case ASYNC_EXEC_DB_SIMPLE: {
      StoolapDB* db = (StoolapDB*)task->target;
      rc = S.exec(db, task->sql, &task->affected);
      if (rc != STOOLAP_OK) task->base.error = dup_cstr(S.errmsg(db), "Exec error");
      break;
    }
    case ASYNC_EXEC_TX: {
      StoolapTx* tx = (StoolapTx*)task->target;
      rc = task->val_count > 0
        ? S.tx_exec_p(tx, task->sql, task->vals, task->val_count, &task->affected)
        : S.tx_exec(tx, task->sql, &task->affected);
      if (rc != STOOLAP_OK) task->base.error = dup_cstr(S.tx_errmsg(tx), "Transaction exec error");
      break;
    }
    case ASYNC_EXEC_STMT: {
      StoolapStmt* stmt = (StoolapStmt*)task->target;
      rc = S.stmt_exec(stmt, task->vals, task->val_count, &task->affected);
      if (rc != STOOLAP_OK) task->base.error = dup_cstr(S.stmt_errmsg(stmt), "Statement exec error");
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
  int32_t rc = STOOLAP_ERR;

  if (task->kind == ASYNC_QUERY_DB) {
    StoolapDB* db = (StoolapDB*)task->target;
    rc = task->val_count > 0
      ? S.query_p(db, task->sql, task->vals, task->val_count, &rows)
      : S.query(db, task->sql, &rows);
    if (rc != STOOLAP_OK) {
      task->base.error = dup_cstr(S.errmsg(db), "Query error");
      return;
    }
  } else if (task->kind == ASYNC_QUERY_TX) {
    StoolapTx* tx = (StoolapTx*)task->target;
    rc = task->val_count > 0
      ? S.tx_query_p(tx, task->sql, task->vals, task->val_count, &rows)
      : S.tx_query(tx, task->sql, &rows);
    if (rc != STOOLAP_OK) {
      task->base.error = dup_cstr(S.tx_errmsg(tx), "Transaction query error");
      return;
    }
  } else {
    StoolapStmt* stmt = (StoolapStmt*)task->target;
    rc = S.stmt_query(stmt, task->vals, task->val_count, &rows);
    if (rc != STOOLAP_OK) {
      task->base.error = dup_cstr(S.stmt_errmsg(stmt), "Statement query error");
      return;
    }
  }

  rc = S.rows_fetch_all(rows, &task->buf, &task->buf_len);
  if (rc != STOOLAP_OK) {
    /* a runtime error while the rows were produced, e.g. an invalid REGEXP */
    task->base.error = dup_cstr(S.rows_errmsg(rows), "Failed to fetch query results");
  }
  S.rows_close(rows);
  if (rc != STOOLAP_OK) {
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
  if (rc != STOOLAP_OK) {
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

  if (rc != STOOLAP_OK) {
    /* the tx handle is consumed by commit/rollback; the message is in the
     * thread-local error of this worker */
    task->base.error = dup_cstr(S.errmsg(NULL),
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
  LOAD(clone,         "stoolap_clone");
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
  LOAD(tx_stmt_exec,  "stoolap_tx_stmt_exec");

  LOAD(tx_commit,     "stoolap_tx_commit");
  LOAD(tx_rollback,   "stoolap_tx_rollback");
  LOAD(tx_errmsg,     "stoolap_tx_errmsg");
  LOAD(rows_close,    "stoolap_rows_close");
  LOAD(rows_fetch_all,"stoolap_rows_fetch_all");
  LOAD(buffer_free,   "stoolap_buffer_free");
  LOAD(rows_errmsg,   "stoolap_rows_errmsg");
  LOAD(rows_fetch_chunk, "stoolap_rows_fetch_chunk");
  LOAD(exec_named,    "stoolap_exec_named");
  LOAD(query_named,   "stoolap_query_named");
  LOAD(tx_exec_named, "stoolap_tx_exec_named");
  LOAD(tx_query_named,"stoolap_tx_query_named");
  LOAD(stmt_query_all,   "stoolap_stmt_query_all");
  LOAD(query_params_all, "stoolap_query_params_all");
  LOAD(query_named_all,  "stoolap_query_named_all");

  #undef LOAD

  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
}

/* setRowFactoryMaker(fn): keeps lib/parse.js makeRowFactory for packed_result */
static napi_value fn_set_row_factory_maker(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  if (row_factory_maker) napi_delete_reference(env, row_factory_maker);
  napi_create_reference(env, argv[0], 1, &row_factory_maker);
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

  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(NULL);
    THROW(env, msg ? msg : "Failed to open database");
  }

  napi_value ext;
  napi_create_external(env, db, NULL, NULL, &ext);
  return ext;
}

/* dbClone(external): external */
static napi_value fn_db_clone(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  StoolapDB* cloned = NULL;
  int32_t rc = S.clone(db, &cloned);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Failed to clone database");
  }

  napi_value ext;
  napi_create_external(env, cloned, NULL, NULL, &ext);
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
  ParamBind pb;
  if (bind_positional(env, argv[2], &pb) != 0) return NULL;
  int64_t affected = 0;
  int32_t rc = pb.count > 0
    ? S.exec_p(db, sql, pb.vals, pb.count, &affected)
    : S.exec(db, sql, &affected);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Exec error");
  }
  return make_changes(env, affected);
}

/* dbExecNamed(external, sql, names, values): { changes } */
static napi_value fn_db_exec_named(napi_env env, napi_callback_info info) {
  size_t argc = 4;
  napi_value argv[4];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  GET_SQL_STRING(env, argv[1], sql);
  NamedBind nb;
  if (bind_named(env, argv[2], argv[3], &nb) != 0) return NULL;
  int64_t affected = 0;
  int32_t rc = S.exec_named(db, sql, nb.np, nb.count, &affected);
  named_release(&nb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Exec error");
  }
  return make_changes(env, affected);
}

/* dbQueryAll(external, tx?, sql, params?, mode): Array | Object | null | ArrayBuffer
 * One FFI call: no rows handle, the buffer belongs to the database handle. */
static napi_value fn_db_query_all(napi_env env, napi_callback_info info) {
  size_t argc = 5;
  napi_value argv[5];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  StoolapTx* tx = tx_arg(env, argv[1]);
  GET_SQL_STRING(env, argv[2], sql);
  int32_t mode;
  napi_get_value_int32(env, argv[4], &mode);
  ParamBind pb;
  if (bind_positional(env, argv[3], &pb) != 0) return NULL;
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t rc = S.query_params_all(db, tx, sql, pb.vals, pb.count, &buf, &len);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }
  return packed_result(env, buf, len, mode, NULL);
}

/* dbQueryNamedAll(external, tx?, sql, names, values, mode) */
static napi_value fn_db_query_named_all(napi_env env, napi_callback_info info) {
  size_t argc = 6;
  napi_value argv[6];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  StoolapTx* tx = tx_arg(env, argv[1]);
  GET_SQL_STRING(env, argv[2], sql);
  int32_t mode;
  napi_get_value_int32(env, argv[5], &mode);
  NamedBind nb;
  if (bind_named(env, argv[3], argv[4], &nb) != 0) return NULL;
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t rc = S.query_named_all(db, tx, sql, nb.np, nb.count, &buf, &len);
  named_release(&nb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }
  return packed_result(env, buf, len, mode, NULL);
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
  if (rc != STOOLAP_OK) {
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

/* dbPrepare(external, sql): [external, boundQueryOneInt] */
static napi_value fn_db_prepare(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  GET_SQL_STRING(env, argv[1], sql);

  StoolapStmt* stmt = NULL;
  int32_t rc = S.prepare(db, sql, &stmt);
  if (rc != STOOLAP_OK) {
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

/* stmtExec(external, params?): { changes } */
static napi_value wrap_stmt_exec(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  GET_STMT(env, argv[0], cs);
  ParamBind pb;
  if (bind_positional(env, argv[1], &pb) != 0) return NULL;
  int64_t affected = 0;
  int32_t rc = S.stmt_exec(cs->stmt, pb.vals, pb.count, &affected);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement exec error");
  }
  return make_changes(env, affected);
}

/* stmtQueryAll(external, params?, mode): Array | Object | null | ArrayBuffer */
static napi_value wrap_stmt_query_all(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  GET_STMT(env, argv[0], cs);
  int32_t mode;
  napi_get_value_int32(env, argv[2], &mode);
  ParamBind pb;
  if (bind_positional(env, argv[1], &pb) != 0) return NULL;
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t rc = S.stmt_query_all(cs->stmt, NULL, pb.vals, pb.count, &buf, &len);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }
  return packed_result(env, buf, len, mode, cs);
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

/* Helper: read numeric args from argv[start..start+count] into StoolapValue array */
static void read_num_args(napi_env env, napi_value* argv, uint32_t start, uint32_t count, StoolapValue* vals) {
  for (uint32_t i = 0; i < count; i++) {
    double d;
    napi_get_value_double(env, argv[start + i], &d);
    if (is_safe_integer(d)) {
      int64_t i64 = (int64_t)d;
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

  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement exec error");
  }

  napi_value result;
  if (affected >= -2147483648LL && affected <= 2147483647LL)
    napi_create_int32(env, (int32_t)affected, &result);
  else
    napi_create_int64(env, affected, &result);
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
  const uint8_t* buf = NULL;
  int64_t blen = 0;
  int32_t rc = S.stmt_query_all(cs->stmt, NULL, vals, (int32_t)len, &buf, &blen);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }
  return packed_result(env, buf, blen, RESULT_ROWS, cs);
}

/* stmtQueryOneNums(external, num1, num2, ...): Object | null */
static napi_value wrap_stmt_query_one_nums(napi_env env, napi_callback_info info) {
  size_t argc = 17;
  napi_value argv[17];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  GET_STMT(env, argv[0], cs);
  uint32_t len = (uint32_t)(argc - 1);
  if (len > 16) len = 16;
  StoolapValue vals[16];
  read_num_args(env, argv, 1, len, vals);
  const uint8_t* buf = NULL;
  int64_t blen = 0;
  int32_t rc = S.stmt_query_all(cs->stmt, NULL, vals, (int32_t)len, &buf, &blen);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }
  return packed_result(env, buf, blen, RESULT_ONE, cs);
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
  if (rc != STOOLAP_OK) {
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
  ParamBind pb;
  if (bind_positional(env, argv[2], &pb) != 0) return NULL;
  int64_t affected = 0;
  int32_t rc = pb.count > 0
    ? S.tx_exec_p(tx, sql, pb.vals, pb.count, &affected)
    : S.tx_exec(tx, sql, &affected);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.tx_errmsg(tx);
    THROW(env, msg ? msg : "Transaction exec error");
  }
  return make_changes(env, affected);
}

/* txExecNamed(external, sql, names, values): { changes } */
static napi_value wrap_tx_exec_named(napi_env env, napi_callback_info info) {
  size_t argc = 4;
  napi_value argv[4];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);
  GET_SQL_STRING(env, argv[1], sql);
  NamedBind nb;
  if (bind_named(env, argv[2], argv[3], &nb) != 0) return NULL;
  int64_t affected = 0;
  int32_t rc = S.tx_exec_named(tx, sql, nb.np, nb.count, &affected);
  named_release(&nb);
  if (rc != STOOLAP_OK) {
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

/* txCommit(external): void */
static napi_value wrap_tx_commit(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapTx* tx;
  napi_get_value_external(env, argv[0], (void**)&tx);

  int32_t rc = S.tx_commit(tx);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(NULL);
    THROW(env, msg && msg[0] ? msg : "Failed to commit transaction");
  }

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
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(NULL);
    THROW(env, msg && msg[0] ? msg : "Failed to rollback transaction");
  }

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
  size_t argc = 4;
  napi_value argv[4];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);

  StoolapTx* tx;
  napi_get_value_external(env, argv[1], (void**)&tx);

  GET_SQL_STRING(env, argv[2], sql);

  uint32_t batch_len;
  napi_get_array_length(env, argv[3], &batch_len);
  if (batch_len == 0) return make_changes(env, 0);

  /* Prepare statement once if tx_stmt_exec is available */
  StoolapStmt* stmt = NULL;
  int32_t rc;
  rc = S.prepare(db, sql, &stmt);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(db);
    THROW(env, msg ? msg : "Prepare error");
  }

  /* Get param count from first element — pre-allocate ONCE */
  napi_value first_arr;
  napi_get_element(env, argv[3], 0, &first_arr);
  uint32_t param_count;
  napi_get_array_length(env, first_arr, &param_count);

  StoolapValue* vals = calloc(param_count, sizeof(StoolapValue));
  TextArena ta;
  ARENA_INIT(ta);

  int64_t total_affected = 0;

  for (uint32_t i = 0; i < batch_len; i++) {
    napi_value params_arr;
    napi_get_element(env, argv[3], i, &params_arr);

    /* Convert params into pre-allocated buffers */
    for (uint32_t j = 0; j < param_count; j++) {
      napi_value elem;
      napi_get_element(env, params_arr, j, &elem);
      if (js_to_value(env, elem, &vals[j], &ta) != 0) {
        arena_reset(&ta);
        free(vals);
        if (stmt) S.stmt_finalize(stmt);
        THROW(env, "Failed to convert parameters");
      }
    }

    int64_t affected = 0;
    rc = S.tx_stmt_exec(tx, stmt, vals, (int32_t)param_count, &affected);

    arena_reset(&ta);

    if (rc != STOOLAP_OK) {
      ERR_SAVE(S.tx_errmsg(tx), "Transaction batch exec error");
      free(vals);
      if (stmt) S.stmt_finalize(stmt);
      THROW(env, _eb);
    }

    total_affected += affected;
  }

  free(vals);
  if (stmt) S.stmt_finalize(stmt);
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
#define PBUF_TS    6  /* i64 nanoseconds since the epoch */
#define PBUF_BLOB  7  /* u32 len + bytes */

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
        if (is_safe_integer(d)) {
          vals[count].value_type = T_INTEGER;
          vals[count]._padding = 0;
          vals[count].v.integer = (int64_t)d;
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
      case PBUF_TS: {
        if (ptr + 8 > end) return -1;
        int64_t nanos; memcpy(&nanos, ptr, 8); ptr += 8;
        vals[count].value_type = T_TIMESTAMP;
        vals[count]._padding = 0;
        vals[count].v.timestamp_nanos = nanos;
        break;
      }
      case PBUF_BLOB: {
        if (ptr + 4 > end) return -1;
        uint32_t len; memcpy(&len, ptr, 4); ptr += 4;
        if (ptr + len > end) return -1;
        vals[count].value_type = T_BLOB;
        vals[count]._padding = 0;
        vals[count].v.blob.ptr = ptr;
        vals[count].v.blob.len = (int64_t)len;
        ptr += len;
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
  if (ptr + 6 > end) THROW(env, "Batch buffer too short");
  uint32_t row_count; memcpy(&row_count, ptr, 4); ptr += 4;
  uint16_t ppr; memcpy(&ppr, ptr, 2); ptr += 2;
  if (row_count == 0) return make_changes(env, 0);
  StoolapValue stack_vals[MAX_STACK_PARAMS];
  StoolapValue* vals = ppr <= MAX_STACK_PARAMS ? stack_vals : malloc(ppr * sizeof(StoolapValue));
  if (!vals) THROW(env, "Out of memory");
#define BATCH_VALS_RELEASE() do { if (vals != stack_vals) free(vals); } while (0)
  StoolapTx* tx = NULL;
  int32_t rc = S.begin(db, &tx);
  if (rc != STOOLAP_OK) { BATCH_VALS_RELEASE(); const char* msg = S.errmsg(db); THROW(env, msg ? msg : "Begin error"); }
  StoolapStmt* stmt = NULL;
  rc = S.prepare(db, sql, &stmt);
  if (rc != STOOLAP_OK) { ERR_SAVE(S.errmsg(db), "Prepare error"); S.tx_rollback(tx); BATCH_VALS_RELEASE(); THROW(env, _eb); }
  int64_t total = 0;
  for (uint32_t i = 0; i < row_count; i++) {
    size_t consumed = 0;
    int decoded = decode_params_buf(ptr, (size_t)(end - ptr), vals, ppr, &consumed);
    if (decoded != ppr) { S.stmt_finalize(stmt); S.tx_rollback(tx); BATCH_VALS_RELEASE(); THROW(env, "Batch param decode error"); }
    ptr += consumed;
    int64_t affected = 0;
    rc = S.tx_stmt_exec(tx, stmt, vals, decoded, &affected);
    if (rc != STOOLAP_OK) { ERR_SAVE(S.tx_errmsg(tx), "Batch exec error"); S.stmt_finalize(stmt); S.tx_rollback(tx); BATCH_VALS_RELEASE(); THROW(env, _eb); }
    total += affected;
  }
  S.stmt_finalize(stmt);
  BATCH_VALS_RELEASE();
  rc = S.tx_commit(tx);
  if (rc != STOOLAP_OK) {
    const char* msg = S.errmsg(NULL);
    THROW(env, msg && msg[0] ? msg : "Commit error");
  }
  return make_changes(env, total);
#undef BATCH_VALS_RELEASE
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

  if (ptr + 6 > end) THROW(env, "Batch buffer too short");
  uint32_t row_count; memcpy(&row_count, ptr, 4); ptr += 4;
  uint16_t ppr; memcpy(&ppr, ptr, 2); ptr += 2;
  if (row_count == 0) return make_changes(env, 0);
  StoolapValue stack_vals[MAX_STACK_PARAMS];
  StoolapValue* vals = ppr <= MAX_STACK_PARAMS ? stack_vals : malloc(ppr * sizeof(StoolapValue));
  if (!vals) THROW(env, "Out of memory");

  int64_t total = 0;
  for (uint32_t i = 0; i < row_count; i++) {
    size_t consumed = 0;
    int decoded = decode_params_buf(ptr, (size_t)(end - ptr), vals, ppr, &consumed);
    if (decoded != ppr) { if (vals != stack_vals) free(vals); THROW(env, "Batch param decode error"); }
    ptr += consumed;
    int64_t affected = 0;
    int32_t rc = S.stmt_exec(stmt, vals, decoded, &affected);
    if (rc != STOOLAP_OK) {
      ERR_SAVE(S.stmt_errmsg(stmt), "Statement batch exec error");
      if (vals != stack_vals) free(vals);
      THROW(env, _eb);
    }
    total += affected;
  }
  if (vals != stack_vals) free(vals);
  return make_changes(env, total);
}

/* ---- Row cursor: a StoolapRows handle read in packed chunks ---- */
/* The external's payload: rows is NULL once the cursor was closed, so the
 * finalizer does not close it a second time. */
typedef struct {
  StoolapRows* rows;
} RowsHandle;

static void finalize_rows_handle(napi_env env, void* data, void* hint) {
  (void)env; (void)hint;
  RowsHandle* h = (RowsHandle*)data;
  if (h->rows) S.rows_close(h->rows);
  free(h);
}

static napi_value rows_external(napi_env env, StoolapRows* rows) {
  RowsHandle* h = malloc(sizeof(RowsHandle));
  if (!h) { S.rows_close(rows); THROW(env, "Out of memory"); }
  h->rows = rows;
  napi_value ext;
  napi_create_external(env, h, finalize_rows_handle, NULL, &ext);
  return ext;
}

/* rowsOpen(db, tx?, sql, params?): external */
static napi_value fn_rows_open(napi_env env, napi_callback_info info) {
  size_t argc = 4;
  napi_value argv[4];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  StoolapTx* tx = tx_arg(env, argv[1]);
  GET_SQL_STRING(env, argv[2], sql);
  ParamBind pb;
  if (bind_positional(env, argv[3], &pb) != 0) return NULL;
  StoolapRows* rows = NULL;
  int32_t rc;
  if (tx) {
    rc = pb.count > 0 ? S.tx_query_p(tx, sql, pb.vals, pb.count, &rows) : S.tx_query(tx, sql, &rows);
  } else {
    rc = pb.count > 0 ? S.query_p(db, sql, pb.vals, pb.count, &rows) : S.query(db, sql, &rows);
  }
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = tx ? S.tx_errmsg(tx) : S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }
  return rows_external(env, rows);
}

/* rowsOpenNamed(db, tx?, sql, names, values): external */
static napi_value fn_rows_open_named(napi_env env, napi_callback_info info) {
  size_t argc = 5;
  napi_value argv[5];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  StoolapDB* db;
  napi_get_value_external(env, argv[0], (void**)&db);
  StoolapTx* tx = tx_arg(env, argv[1]);
  GET_SQL_STRING(env, argv[2], sql);
  NamedBind nb;
  if (bind_named(env, argv[3], argv[4], &nb) != 0) return NULL;
  StoolapRows* rows = NULL;
  int32_t rc = tx
    ? S.tx_query_named(tx, sql, nb.np, nb.count, &rows)
    : S.query_named(db, sql, nb.np, nb.count, &rows);
  named_release(&nb);
  if (rc != STOOLAP_OK) {
    const char* msg = tx ? S.tx_errmsg(tx) : S.errmsg(db);
    THROW(env, msg ? msg : "Query error");
  }
  return rows_external(env, rows);
}

/* rowsOpenStmt(external, params?): external */
static napi_value wrap_rows_open_stmt(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  GET_STMT(env, argv[0], cs);
  ParamBind pb;
  if (bind_positional(env, argv[1], &pb) != 0) return NULL;
  StoolapRows* rows = NULL;
  int32_t rc = S.stmt_query(cs->stmt, pb.vals, pb.count, &rows);
  bind_release(&pb);
  if (rc != STOOLAP_OK) {
    const char* msg = S.stmt_errmsg(cs->stmt);
    THROW(env, msg ? msg : "Statement query error");
  }
  return rows_external(env, rows);
}

/* rowsChunk(external, maxRows, buffer?): ArrayBuffer | null
 * Packs up to maxRows rows into `buffer` when it is large enough, else into
 * a new ArrayBuffer sized to grow; the caller keeps the returned buffer for
 * the next call. null once the result is exhausted. */
static napi_value fn_rows_chunk(napi_env env, napi_callback_info info) {
  size_t argc = 3;
  napi_value argv[3];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  RowsHandle* h;
  napi_get_value_external(env, argv[0], (void**)&h);
  StoolapRows* rows = h->rows;
  int32_t max_rows;
  napi_get_value_int32(env, argv[1], &max_rows);
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t n = 0;
  if (rows && S.rows_fetch_chunk(rows, max_rows, &buf, &len, &n) != STOOLAP_OK) {
    const char* msg = S.rows_errmsg(rows);
    THROW(env, msg ? msg : "Row fetch error");
  }
  if (n == 0) {
    napi_value null_val;
    napi_get_null(env, &null_val);
    return null_val;
  }
  napi_value ab = argv[2];
  void* data = NULL;
  size_t cap = 0;
  bool is_ab = false;
  napi_is_arraybuffer(env, ab, &is_ab);
  if (is_ab) napi_get_arraybuffer_info(env, ab, &data, &cap);
  if (!is_ab || cap < (size_t)len) {
    cap = (size_t)len < 16384 ? 16384 : (size_t)len * 2;
    napi_create_arraybuffer(env, cap, &data, &ab);
  }
  memcpy(data, buf, (size_t)len);
  return ab;
}

typedef struct {
  AsyncBase base;
  RowsHandle* h;
  int32_t max_rows;
  uint8_t* buf;
  int64_t len;
} AsyncChunkTask;

static void execute_rows_chunk_async(napi_env env, void* data) {
  (void)env;
  AsyncChunkTask* task = (AsyncChunkTask*)data;
  const uint8_t* buf = NULL;
  int64_t len = 0;
  int32_t n = 0;
  StoolapRows* rows = task->h->rows;
  if (!rows) return;
  if (S.rows_fetch_chunk(rows, task->max_rows, &buf, &len, &n) != STOOLAP_OK) {
    task->base.error = dup_cstr(S.rows_errmsg(rows), "Row fetch error");
    return;
  }
  if (n == 0) return;
  task->buf = malloc((size_t)len);
  if (!task->buf) { task->base.error = dup_cstr(NULL, "Out of memory"); return; }
  memcpy(task->buf, buf, (size_t)len);
  task->len = len;
}

static void finalize_malloc_buffer(napi_env env, void* data, void* hint) {
  (void)env; (void)hint;
  free(data);
}

static void complete_rows_chunk_async(napi_env env, napi_status status, void* data) {
  AsyncChunkTask* task = (AsyncChunkTask*)data;
  if (status != napi_ok && !task->base.error) {
    task->base.error = dup_cstr(NULL, "Row fetch cancelled");
  }
  if (task->base.error) {
    free(task->buf);
    reject_async_error(env, &task->base, "Row fetch error");
  } else if (!task->buf) {
    napi_value null_val;
    napi_get_null(env, &null_val);
    napi_resolve_deferred(env, task->base.deferred, null_val);
  } else {
    napi_value ab;
    if (napi_create_external_arraybuffer(env, task->buf, (size_t)task->len,
                                         finalize_malloc_buffer, NULL, &ab) != napi_ok) {
      void* copy;
      napi_create_arraybuffer(env, (size_t)task->len, &copy, &ab);
      memcpy(copy, task->buf, (size_t)task->len);
      free(task->buf);
    }
    napi_resolve_deferred(env, task->base.deferred, ab);
  }
  napi_delete_async_work(env, task->base.work);
  free(task->base.error);
  free(task);
}

/* rowsChunkAsync(external, maxRows): Promise<ArrayBuffer | null> */
static napi_value fn_rows_chunk_async(napi_env env, napi_callback_info info) {
  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  AsyncChunkTask* task = calloc(1, sizeof(AsyncChunkTask));
  if (!task) THROW(env, "Out of memory");
  napi_get_value_external(env, argv[0], (void**)&task->h);
  napi_get_value_int32(env, argv[1], &task->max_rows);
  napi_value promise = queue_async_work(
    env, task, &task->base, "rowsChunkAsync", execute_rows_chunk_async, complete_rows_chunk_async);
  if (!promise) {
    free(task);
    THROW(env, "Failed to queue rowsChunkAsync");
  }
  return promise;
}

/* rowsClose(external): closes the rows handle; later calls on it are no-ops */
static napi_value fn_rows_cursor_close(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  RowsHandle* h;
  napi_get_value_external(env, argv[0], (void**)&h);
  if (h->rows) {
    S.rows_close(h->rows);
    h->rows = NULL;
  }
  napi_value undef;
  napi_get_undefined(env, &undef);
  return undef;
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
  EXPORT_FN(env, exports, "setRowFactoryMaker", fn_set_row_factory_maker);
  EXPORT_FN(env, exports, "dbOpen",        fn_db_open);
  EXPORT_FN(env, exports, "dbOpenAsync",   fn_db_open_async);
  EXPORT_FN(env, exports, "dbClone",       fn_db_clone);
  EXPORT_FN(env, exports, "dbClose",       fn_db_close);
  EXPORT_FN(env, exports, "dbCloseAsync",  fn_db_close_async);
  EXPORT_FN(env, exports, "dbExec",        fn_db_exec);
  EXPORT_FN(env, exports, "dbExecAsync",   fn_db_exec_async);
  EXPORT_FN(env, exports, "dbExecNamed",   fn_db_exec_named);
  EXPORT_FN(env, exports, "dbQueryAll",    fn_db_query_all);
  EXPORT_FN(env, exports, "dbQueryNamedAll", fn_db_query_named_all);
  EXPORT_FN(env, exports, "dbExecSimple",  fn_db_exec_simple);
  EXPORT_FN(env, exports, "dbExecSimpleAsync", fn_db_exec_simple_async);
  EXPORT_FN(env, exports, "dbQueryBufAsync", fn_db_query_buf_async);
  EXPORT_FN(env, exports, "dbPrepare",     fn_db_prepare);
  EXPORT_FN(env, exports, "stmtExec",      wrap_stmt_exec);
  EXPORT_FN(env, exports, "stmtExecAsync", wrap_stmt_exec_async);
  EXPORT_FN(env, exports, "stmtQueryAll",  wrap_stmt_query_all);
  EXPORT_FN(env, exports, "stmtQueryBufAsync", wrap_stmt_query_buf_async);
  EXPORT_FN(env, exports, "stmtExecNums",     wrap_stmt_exec_nums);
  EXPORT_FN(env, exports, "stmtQueryNums",    wrap_stmt_query_nums);
  EXPORT_FN(env, exports, "stmtQueryOneNums", wrap_stmt_query_one_nums);
  EXPORT_FN(env, exports, "stmtExecBatchBuf", wrap_stmt_exec_batch_buf);
  EXPORT_FN(env, exports, "stmtFinalize",  wrap_stmt_finalize);
  EXPORT_FN(env, exports, "dbExecBatchBuf",   wrap_db_exec_batch_buf);
  EXPORT_FN(env, exports, "txBegin",       fn_tx_begin);
  EXPORT_FN(env, exports, "txBeginAsync",  fn_tx_begin_async);
  EXPORT_FN(env, exports, "txExec",        wrap_tx_exec);
  EXPORT_FN(env, exports, "txExecAsync",   wrap_tx_exec_async);
  EXPORT_FN(env, exports, "txExecNamed",   wrap_tx_exec_named);
  EXPORT_FN(env, exports, "txExecBatch",   wrap_tx_exec_batch);
  EXPORT_FN(env, exports, "txQueryBufAsync", wrap_tx_query_buf_async);
  EXPORT_FN(env, exports, "txCommit",      wrap_tx_commit);
  EXPORT_FN(env, exports, "txCommitAsync", wrap_tx_commit_async);
  EXPORT_FN(env, exports, "txRollback",    wrap_tx_rollback);
  EXPORT_FN(env, exports, "txRollbackAsync", wrap_tx_rollback_async);
  EXPORT_FN(env, exports, "rowsOpen",      fn_rows_open);
  EXPORT_FN(env, exports, "rowsOpenNamed", fn_rows_open_named);
  EXPORT_FN(env, exports, "rowsOpenStmt",  wrap_rows_open_stmt);
  EXPORT_FN(env, exports, "rowsChunk",     fn_rows_chunk);
  EXPORT_FN(env, exports, "rowsChunkAsync", fn_rows_chunk_async);
  EXPORT_FN(env, exports, "rowsClose",     fn_rows_cursor_close);
  return exports;
}

NAPI_MODULE(stoolap, Init)
