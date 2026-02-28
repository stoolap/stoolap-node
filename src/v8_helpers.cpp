// Copyright 2025 Stoolap Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// v8_helpers.cpp — Direct V8 bulk object creation for stoolap-node
//
// Bypasses NAPI per-property overhead by using:
//   v8::Object::New(isolate, proto, keys, values, count)  — one hidden class
//   v8::String::NewFromUtf8(..., kInternalized)            — cached column names
//   v8::Array::New(isolate, elements, count)               — bulk array
//
// Called from Rust via extern "C" FFI.

#include <v8.h>
#include <node_api.h>
#include <cstring>

// Cell type tags — must match Rust #[repr(u8)] CellTag
enum CellTag : uint8_t {
    TAG_NULL          = 0,
    TAG_BOOL_FALSE    = 1,
    TAG_BOOL_TRUE     = 2,
    TAG_INT32         = 3,
    TAG_DOUBLE        = 4,
    TAG_STRING        = 5,
    TAG_INT64         = 6,
    TAG_FLOAT32_ARRAY = 7,
};

// C-compatible cell data — must match Rust #[repr(C)] CellData layout
struct CellData {
    uint8_t tag;
    // 7 bytes padding (alignment for int_val)
    int64_t int_val;
    double  float_val;
    const char* str_ptr;
    int32_t str_len;
    // 4 bytes padding (struct alignment)
};

// ----------------------------------------------------------------
// v8::Local -> napi_value conversion
// napi_value is reinterpret_cast<napi_value>(*local) internally
// ----------------------------------------------------------------

static inline napi_value from_v8(v8::Local<v8::Value> local) {
    return reinterpret_cast<napi_value>(*local);
}

// ----------------------------------------------------------------
// Convert CellData to v8::Value using direct V8 API (~5ns vs ~30ns NAPI)
// ----------------------------------------------------------------

static inline v8::Local<v8::Value> cell_to_v8(v8::Isolate* isolate,
                                               const CellData& cell) {
    switch (cell.tag) {
        case TAG_NULL:
            return v8::Null(isolate);
        case TAG_BOOL_FALSE:
            return v8::Boolean::New(isolate, false);
        case TAG_BOOL_TRUE:
            return v8::Boolean::New(isolate, true);
        case TAG_INT32:
            return v8::Int32::New(isolate, static_cast<int32_t>(cell.int_val));
        case TAG_DOUBLE:
            return v8::Number::New(isolate, cell.float_val);
        case TAG_STRING:
            return v8::String::NewFromUtf8(
                isolate, cell.str_ptr,
                v8::NewStringType::kNormal, cell.str_len
            ).ToLocalChecked();
        case TAG_INT64:
            // Large integers outside i32 range — still a JS Number (double).
            // Matches napi_create_int64 behavior (converts to double).
            return v8::Number::New(isolate, static_cast<double>(cell.int_val));
        case TAG_FLOAT32_ARRAY: {
            // Vector: str_ptr = packed LE f32 bytes, str_len = byte count
            int byte_len = cell.str_len;
            auto backing = v8::ArrayBuffer::New(isolate, byte_len);
            memcpy(backing->GetBackingStore()->Data(), cell.str_ptr, byte_len);
            return v8::Float32Array::New(backing, 0, byte_len / 4);
        }
        default:
            return v8::Null(isolate);
    }
}

extern "C" {

// Create a single row object using V8 bulk API.
// Used for queryOne (single-row results).
napi_value v8_create_single_object(
    int col_count,
    const char* const* col_ptrs,
    const int* col_lens,
    const CellData* cells
) {
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);

    if (col_count == 0) {
        return from_v8(scope.Escape(v8::Object::New(isolate)));
    }

    // Create internalized column name strings
    v8::LocalVector<v8::Name> keys(isolate);
    keys.reserve(col_count);
    for (int c = 0; c < col_count; c++) {
        auto name = v8::String::NewFromUtf8(
            isolate, col_ptrs[c],
            v8::NewStringType::kInternalized, col_lens[c]
        ).ToLocalChecked();
        keys.push_back(name.As<v8::Name>());
    }

    // Convert cell values
    v8::LocalVector<v8::Value> vals(isolate);
    vals.reserve(col_count);
    for (int c = 0; c < col_count; c++) {
        vals.push_back(cell_to_v8(isolate, cells[c]));
    }

    // Get Object.prototype
    auto ctx = isolate->GetCurrentContext();
    auto global = ctx->Global();
    auto obj_str = v8::String::NewFromUtf8(
        isolate, "Object", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto obj_ctor = global->Get(ctx, obj_str).ToLocalChecked();
    auto proto_str = v8::String::NewFromUtf8(
        isolate, "prototype", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto proto = v8::Local<v8::Object>::Cast(obj_ctor)
        ->Get(ctx, proto_str).ToLocalChecked();

    auto obj = v8::Object::New(
        isolate, proto, keys.data(), vals.data(), col_count
    );
    return from_v8(scope.Escape(obj));
}

// Create a JS null value (for queryOne returning no row).
napi_value v8_create_null(void) {
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);
    return from_v8(scope.Escape(v8::Null(isolate)));
}

// ----------------------------------------------------------------
// Streaming row creation — C++ calls Rust callback per row.
// Eliminates Value cloning: reads directly from current_row().
// ----------------------------------------------------------------

// Callback type: advance to next row, fill cells.
// Returns 1 if row available, 0 when exhausted.
typedef int (*RowCallback)(void* ctx, CellData* out_cells);

napi_value v8_create_rows_streaming(
    int col_count,
    const char* const* col_ptrs,
    const int* col_lens,
    RowCallback next_row,
    void* ctx
) {
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);

    if (col_count == 0) {
        auto arr = v8::Array::New(isolate, 0);
        return from_v8(scope.Escape(arr));
    }

    // Create internalized column name strings (cached by V8)
    v8::LocalVector<v8::Name> keys(isolate);
    keys.reserve(col_count);
    for (int c = 0; c < col_count; c++) {
        auto name = v8::String::NewFromUtf8(
            isolate, col_ptrs[c],
            v8::NewStringType::kInternalized, col_lens[c]
        ).ToLocalChecked();
        keys.push_back(name.As<v8::Name>());
    }

    // Get Object.prototype
    auto v8_ctx = isolate->GetCurrentContext();
    auto global = v8_ctx->Global();
    auto obj_str = v8::String::NewFromUtf8(
        isolate, "Object", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto obj_ctor = global->Get(v8_ctx, obj_str).ToLocalChecked();
    auto proto_str = v8::String::NewFromUtf8(
        isolate, "prototype", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto proto = v8::Local<v8::Object>::Cast(obj_ctor)
        ->Get(v8_ctx, proto_str).ToLocalChecked();

    // Reusable per-row cell buffer (stack-sized for typical queries)
    CellData cells_buf[64];
    CellData* cells = (col_count <= 64) ? cells_buf : new CellData[col_count];

    v8::LocalVector<v8::Value> rows(isolate);
    v8::LocalVector<v8::Value> vals(isolate);
    vals.reserve(col_count);

    while (next_row(ctx, cells) != 0) {
        vals.clear();
        for (int c = 0; c < col_count; c++) {
            vals.push_back(cell_to_v8(isolate, cells[c]));
        }
        auto obj = v8::Object::New(
            isolate, proto, keys.data(), vals.data(), col_count
        );
        rows.push_back(obj);
    }

    if (cells != cells_buf) {
        delete[] cells;
    }

    auto arr = v8::Array::New(isolate, rows.data(), rows.size());
    return from_v8(scope.Escape(arr));
}

// ----------------------------------------------------------------
// Raw format: { columns: string[], rows: any[][] }
// Same streaming callback, but rows are arrays instead of objects.
// ----------------------------------------------------------------

napi_value v8_create_raw_streaming(
    int col_count,
    const char* const* col_ptrs,
    const int* col_lens,
    RowCallback next_row,
    void* ctx
) {
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);

    // Create columns array
    v8::LocalVector<v8::Value> col_names(isolate);
    col_names.reserve(col_count);
    for (int c = 0; c < col_count; c++) {
        auto name = v8::String::NewFromUtf8(
            isolate, col_ptrs[c],
            v8::NewStringType::kInternalized, col_lens[c]
        ).ToLocalChecked();
        col_names.push_back(name);
    }
    auto columns_arr = v8::Array::New(isolate, col_names.data(), col_names.size());

    // Stream rows as arrays of values
    CellData cells_buf[64];
    CellData* cells = (col_count <= 64) ? cells_buf : new CellData[col_count];

    v8::LocalVector<v8::Value> rows(isolate);
    v8::LocalVector<v8::Value> vals(isolate);
    vals.reserve(col_count);

    while (next_row(ctx, cells) != 0) {
        vals.clear();
        for (int c = 0; c < col_count; c++) {
            vals.push_back(cell_to_v8(isolate, cells[c]));
        }
        auto row_arr = v8::Array::New(isolate, vals.data(), vals.size());
        rows.push_back(row_arr);
    }

    if (cells != cells_buf) {
        delete[] cells;
    }

    auto rows_arr = v8::Array::New(isolate, rows.data(), rows.size());

    // Create { columns, rows } result object
    auto v8_ctx = isolate->GetCurrentContext();
    auto global = v8_ctx->Global();
    auto obj_str = v8::String::NewFromUtf8(
        isolate, "Object", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto obj_ctor = global->Get(v8_ctx, obj_str).ToLocalChecked();
    auto proto_str = v8::String::NewFromUtf8(
        isolate, "prototype", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto proto = v8::Local<v8::Object>::Cast(obj_ctor)
        ->Get(v8_ctx, proto_str).ToLocalChecked();

    v8::LocalVector<v8::Name> keys(isolate);
    v8::LocalVector<v8::Value> values(isolate);
    keys.push_back(v8::String::NewFromUtf8(
        isolate, "columns", v8::NewStringType::kInternalized
    ).ToLocalChecked().As<v8::Name>());
    keys.push_back(v8::String::NewFromUtf8(
        isolate, "rows", v8::NewStringType::kInternalized
    ).ToLocalChecked().As<v8::Name>());
    values.push_back(columns_arr);
    values.push_back(rows_arr);

    auto result = v8::Object::New(isolate, proto, keys.data(), values.data(), 2);
    return from_v8(scope.Escape(result));
}

// ----------------------------------------------------------------
// RunResult: { changes: number } — single-property object for DML
// Hot path for INSERT/UPDATE/DELETE: one V8 call instead of 3 NAPI calls
// ----------------------------------------------------------------

napi_value v8_create_run_result(int64_t changes) {
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    v8::EscapableHandleScope scope(isolate);

    auto ctx = isolate->GetCurrentContext();
    auto global = ctx->Global();
    auto obj_str = v8::String::NewFromUtf8(
        isolate, "Object", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto obj_ctor = global->Get(ctx, obj_str).ToLocalChecked();
    auto proto_str = v8::String::NewFromUtf8(
        isolate, "prototype", v8::NewStringType::kInternalized
    ).ToLocalChecked();
    auto proto = v8::Local<v8::Object>::Cast(obj_ctor)
        ->Get(ctx, proto_str).ToLocalChecked();

    v8::LocalVector<v8::Name> keys(isolate);
    v8::LocalVector<v8::Value> values(isolate);

    keys.push_back(v8::String::NewFromUtf8(
        isolate, "changes", v8::NewStringType::kInternalized
    ).ToLocalChecked().As<v8::Name>());

    if (changes >= INT32_MIN && changes <= INT32_MAX) {
        values.push_back(v8::Int32::New(isolate, static_cast<int32_t>(changes)));
    } else {
        values.push_back(v8::Number::New(isolate, static_cast<double>(changes)));
    }

    auto result = v8::Object::New(isolate, proto, keys.data(), values.data(), 1);
    return from_v8(scope.Escape(result));
}

} // extern "C"
