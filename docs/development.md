# Development

Install [rustup](https://rustup.rs/) before working from a checkout. Cargo uses
[`rust-toolchain.toml`](../rust-toolchain.toml) to select and install the pinned Rust release and Clippy.
Local builds also require FFmpeg 8 development libraries, Clang, pkg-config, and
C/C++ build tools; the [Dockerfile](../Dockerfile) records the Linux build dependencies.

Run the same lint check used by CI before submitting changes:

```bash
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked -p lyra-server
```

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

See [commit conventions](commits.md) when preparing changes.

## Cargo installation

For local development, install the build prerequisites above, then install the server:

```sh
cargo install --locked --git https://git.lyra.pub/lyra/lyra lyra-server
lyra serve
```

For a frontend, build [lyra-web](https://github.com/lyra-org/lyra-web) and set `LYRA_STATIC_DIR` to its output.

See [plugin repositories](plugin-repositories.md) to install plugins.

## Docker builds

Docker builds require a lyra-web commit SHA in `LYRA_WEB_GIT_HASH`. To build with its current `main`, as CI does:

```sh
LYRA_WEB_GIT_HASH=$(git ls-remote https://github.com/lyra-org/lyra-web.git refs/heads/main | cut -f1)
docker build \
  --build-arg LYRA_GIT_HASH="$(git rev-parse HEAD)" \
  --build-arg LYRA_WEB_GIT_HASH="$LYRA_WEB_GIT_HASH" \
  -t lyra:local .
```
