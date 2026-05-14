# Vendored Luau Runtime Dependencies

This directory vendors the Luau runtime dependencies that the Harmony crates
build on:

- `mluau`: `https://github.com/mluau/mluau` at `f4f97c9ac60ba81eceb88002254ec199559bffd8`
- `mlua-sys`: from the same `mluau` checkout
- `mlua_derive`: from the same `mluau` checkout
- `mluau-scheduler`: `https://github.com/mluau/scheduler` at `585795be51809c5f25044b1e833bef42fac77c45`
- `luau-src-rs`: `https://github.com/mluau/luau-src-rs` at `60a200e92afddc9215872f84ec20f78480d1a978`

The root `Cargo.toml` patches the upstream git sources to these local paths.
Update these checkouts intentionally when bumping Luau so runtime, sys, source,
and scheduler changes are reviewed together.
