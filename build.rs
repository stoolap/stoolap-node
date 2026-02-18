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

extern crate napi_build;

fn main() {
    napi_build::setup();

    // Compile v8_helpers.cpp — direct V8 bulk object creation
    let node_include = node_include_dir();
    cc::Build::new()
        .cpp(true)
        .file("src/v8_helpers.cpp")
        .include(&node_include)
        .flag("-std=c++20")
        .flag("-fno-exceptions")
        .flag("-fno-rtti")
        .flag("-Wno-unused-parameter")
        .flag("-O2")
        .compile("v8_helpers");
}

/// Get Node.js include directory for V8 headers.
/// Resolves symlinks to find the actual install prefix.
fn node_include_dir() -> String {
    let output = std::process::Command::new("node")
        .args([
            "-e",
            "const p = require('path'); \
             const fs = require('fs'); \
             const real = fs.realpathSync(process.execPath); \
             console.log(p.resolve(real, '..', '..', 'include', 'node'));",
        ])
        .output()
        .expect("Failed to find Node.js. Make sure `node` is in your PATH.");

    let dir = String::from_utf8(output.stdout)
        .expect("Invalid UTF-8 in Node.js include path")
        .trim()
        .to_string();

    let v8_header = std::path::Path::new(&dir).join("v8.h");
    if !v8_header.exists() {
        panic!(
            "V8 headers not found at {dir}/v8.h. \
             Install Node.js development headers."
        );
    }

    dir
}
