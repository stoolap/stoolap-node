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
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .file("src/v8_helpers.cpp")
        .include(&node_include);

    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("msvc") {
        // MSVC flags
        build.flag("/std:c++20").flag("/EHs-").flag("/GR-");
    } else {
        // GCC/Clang flags
        build
            .flag("-std=c++20")
            .flag("-fno-exceptions")
            .flag("-fno-rtti")
            .flag("-Wno-unused-parameter")
            .flag("-O2");
    }

    build.compile("v8_helpers");
}

/// Get Node.js include directory for V8 headers.
///
/// Checks multiple locations:
/// 1. Standard Node.js install prefix (Linux/macOS with dev headers)
/// 2. node-gyp cache (Windows, or when headers installed via `npx node-gyp install`)
/// 3. Auto-installs headers via node-gyp if not found
fn node_include_dir() -> String {
    let script = r#"
const p = require('path'), fs = require('fs');

function find(dirs) {
    for (const d of dirs) {
        if (d && fs.existsSync(p.join(d, 'v8.h'))) return d;
    }
    return null;
}

const real = fs.realpathSync(process.execPath);
const stdDir = p.resolve(real, '..', '..', 'include', 'node');
const v = process.version.slice(1);
const home = process.env.HOME || '';
const la = process.env.LOCALAPPDATA || '';
const gypDirs = [
    p.join(home, '.node-gyp', v, 'include', 'node'),
    p.join(la, 'node-gyp', 'Cache', v, 'include', 'node'),
];

let dir = find([stdDir, ...gypDirs]);
if (dir) { console.log(dir); process.exit(0); }

// Auto-install headers via node-gyp
try {
    require('child_process').execSync('npx --yes node-gyp install', { stdio: 'inherit' });
} catch {}

dir = find(gypDirs);
if (dir) { console.log(dir); process.exit(0); }

process.exit(1);
"#;

    let output = std::process::Command::new("node")
        .args(["-e", script])
        .output()
        .expect("Failed to find Node.js. Make sure `node` is in your PATH.");

    if output.status.success() {
        return String::from_utf8(output.stdout)
            .expect("Invalid UTF-8 in Node.js include path")
            .trim()
            .to_string();
    }

    panic!(
        "V8 headers not found. Install Node.js development headers \
         or run: npx node-gyp install"
    );
}
