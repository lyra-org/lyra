# Development

Install [rustup](https://rustup.rs/) before working from a checkout. Cargo uses
[`rust-toolchain.toml`](../rust-toolchain.toml) to select and install the pinned Rust release and Clippy.
Local builds also require FFmpeg 8 development libraries, Clang, pkg-config, and
C/C++ build tools; the [Dockerfile](../Dockerfile) records the Linux build dependencies.

Plugin test traces are stored with [Git LFS](https://git-lfs.com/). Run `git lfs install` once
before cloning, or `git lfs pull` in a checkout made without it; otherwise the traces are pointer
files and the plugin scenarios fail.

Before submitting changes, run the lint check used by CI and the server tests:

```bash
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p lyra-server
cargo test --locked -p lyra-server --features docgen
```

The `docgen` run adds the tests that check routes against the generated OpenAPI
document, such as the reserved plugin ids.

To run Clippy with the image’s build dependencies:

```bash
docker build --target clippy .
```

Keep GitLab’s **Pipelines must succeed** merge check enabled, including on forks.

Rust formatting uses nightly separately from the pinned build toolchain:

```bash
rustup toolchain install nightly --component rustfmt
cargo +nightly fmt
```

When updating `rust-toolchain.toml`, resolve new lint and test failures in the same change.

See [testing](testing.md) for plugin scenarios and Luau tests, and [commit conventions](commits.md) when preparing changes.

## Cargo installation

For local development, install the build prerequisites above, then install the server from a checkout so the pinned toolchain applies:

```sh
cargo install --locked --path lyra-server
lyra serve
```

For a frontend, build [lyra-web](https://github.com/lyra-org/lyra-web) and set `LYRA_STATIC_DIR` to its output.

See [plugin repositories](plugin-repositories.md) to install plugins.

## Startup library

To create and scan a library at startup, add a `library` block to [`config.json`](configuration.md):

```json
{
  "library": {
    "path": "/music",
    "name": "Music",
    "language": "en",
    "country": "US"
  }
}
```

Use a path visible to the server. `path` is required. `name` defaults to `Music`. `language` and `country` are optional; language accepts ISO 639-1 or 639-3 codes or English names, and country accepts country codes or names.

## Versions and releases

The `lyra-*` crates share the server version, set once under `[workspace.package]` in the root
`Cargo.toml`. Between releases, `main` carries the next version as a numbered development
version, starting at `-dev.0`, such as `0.0.2-dev.0`, and every plugin manifest in the repository
declares the same `lyra` version. Tests check that they match.

Plugins reach servers only through the releases in `versions.json`, which on `main` are offered to
the development version. To release plugins from `main`:

1. If the server changed since the previous entry's commit, bump the development number, such as
   `0.0.2-dev.0` to `0.0.2-dev.1`, in the workspace and every in-repository `lyra`, so servers built
   before the change keep the plugins that work with them.
2. Bump `version` in each plugin whose files changed since the previous entry's commit.
3. Commit, then add that commit to the top of `versions.json` as described in
   [plugin repositories](plugin-repositories.md#releases).

To release a version:

1. Set the workspace version and every in-repository `lyra` to it, and bump `version` in each plugin whose files changed since the previous release.
2. Commit, and tag that commit `v` followed by the version, such as `v0.0.2`.
3. Add that commit to the top of `versions.json`, and commit.
4. Move `main` to the next development version, such as `0.0.3-dev.0`, in the workspace and every in-repository `lyra`, and commit.
5. Push the commits and the tag together.

CI checks the version line and every `versions.json` entry against the one before it with
[`ci/release-checks.sh`](../ci/release-checks.sh).

The `harmony-*` crates have their own version, released when Harmony is ready rather than with
each Lyra release. Plugins depend on the server version only, so a Lyra release may ship a `-dev`
Harmony version.

## Docker builds

Full image builds require a lyra-web commit SHA in `LYRA_WEB_GIT_HASH`. To build with its current `main`, as CI does:

```sh
LYRA_WEB_GIT_HASH=$(git ls-remote https://github.com/lyra-org/lyra-web.git refs/heads/main | cut -f1)
docker build \
  --build-arg LYRA_GIT_HASH="$(git rev-parse HEAD)" \
  --build-arg LYRA_WEB_GIT_HASH="$LYRA_WEB_GIT_HASH" \
  -t lyra:local .
```
