// This Source Code Form is subject to the terms of the Lyra Public License,
// v1.0. If a copy of the Lyra Public License was not distributed with this file,
// You can obtain one here:
// www.meshiplaw.com/lyra.

//! Compiles the C++ gateway that runs raising Luau API calls under a protected call.

use std::env;

const GATEWAY: &str = "csrc/gateway.cpp";

fn main() {
    println!("cargo:rerun-if-changed={GATEWAY}");

    let include = env::var_os("DEP_LUAU_INCLUDE").expect("harmony-luau-sys exports its headers");
    let mut build = cc::Build::new();
    build.cpp(true).std("c++17").file(GATEWAY);
    for dir in env::split_paths(&include) {
        build.include(dir);
    }
    build.compile("harmony_luau_gateway");
}
