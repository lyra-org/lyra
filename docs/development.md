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

GitLab runs Clippy before publishing images, including on merge request pipelines.
The check uses the Dockerfile's shared native dependencies and can also run locally:

```bash
docker build --target clippy .
```

GitLab's **Pipelines must succeed** merge check must remain enabled to enforce this
gate. Forks must enable it separately; the CI file cannot configure this project setting.

Rust formatting uses nightly separately from the pinned build toolchain:

```bash
rustup toolchain install nightly --component rustfmt
cargo +nightly fmt
```

To upgrade Rust, change the exact release in `rust-toolchain.toml`, run the lint
check and tests above, and resolve any new diagnostics in the same change. Keep
the pin current so compiler fixes and new Clippy checks reach the project regularly.

See [commit conventions](commits.md) when preparing changes.

## Cargo installation

For local development, install the build prerequisites above, then install the server:

```sh
cargo install --locked --git https://git.lyra.pub/lyra/lyra lyra-server
lyra serve
```

The server listens on port 4746 and stores state in `./data` under the working directory. For a frontend, build [lyra-web](https://github.com/lyra-org/lyra-web) and set `LYRA_STATIC_DIR` to its output. Use the music folder's local path when creating the library.

Copy the bundled plugins from this repository's [`plugins`](../plugins) directory into a `plugins` directory where you run the binary, especially the MusicBrainz plugin. See [plugin repositories](plugin-repositories.md) for installing and updating additional plugins.

## Docker builds

The image includes the lyra-web frontend, so the build needs a commit SHA from that repository as `LYRA_WEB_GIT_HASH`. CI uses the current `main` commit. To do the same locally:

```sh
LYRA_WEB_GIT_HASH=$(git ls-remote https://github.com/lyra-org/lyra-web.git refs/heads/main | cut -f1)
docker build \
  --build-arg LYRA_GIT_HASH="$(git rev-parse HEAD)" \
  --build-arg LYRA_WEB_GIT_HASH="$LYRA_WEB_GIT_HASH" \
  -t lyra:local .
```
