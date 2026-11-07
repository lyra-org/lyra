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

For a frontend, build [lyra-web](https://github.com/lyra-org/lyra-web) and set `LYRA_STATIC_DIR` to its output. Settings and `config.json` are described in [configuration](https://lyra.pub/guides/configuration/).

See [plugin repositories](plugin-repositories.md) to install plugins.

## Versions and releases

The `lyra-*` crates share the server version, set once under `[workspace.package]` in the root
`Cargo.toml`. Between releases, `main` carries the next version as a numbered development
version, starting at `-dev.0`, such as `0.0.2-dev.0`, and every plugin manifest in the repository
declares the same `lyra` version. Tests check that they match.

Images of tagged versions ship the [lyra-web](https://github.com/lyra-org/lyra-web) commit in
[`web.rev`](../web.rev); other images ship lyra-web's `main` at build time. Every image records its
lyra-web commit in the `pub.lyra.web.revision` label.

Plugins reach servers only through the releases in `versions.json`, which on `main` are offered to
the development version. When a batch of changes is ready for testers, such as a settled database
change, publish a development version from `main`:

1. Bump the development number, such as `0.0.2-dev.0` to `0.0.2-dev.1`, in the workspace and
   every in-repository `lyra`, bump `version` in each plugin whose files changed since the
   previous `versions.json` entry's commit, and set `web.rev` to the lyra-web commit to ship,
   usually its `main`.
2. Commit as `release: publish 0.0.2-dev.1`, and tag that commit `v0.0.2-dev.1`.
3. If any plugin changed, add that commit to the top of `versions.json` as described in
   [plugin repositories](plugin-repositories.md#releases), and commit as
   `plugins: release the official catalog as 0.0.2-dev.1`.
4. Push the commits and the tag together.

After a release, the first development version published is `-dev.1`; `-dev.0` only marks `main`
until then.

To release a version:

1. Set the workspace version and every in-repository `lyra` to it, bump `version` in each plugin
   whose files changed since the previous release, and set `web.rev` to the lyra-web commit to
   ship.
2. Commit as `release: publish 0.0.2`, and tag that commit `v0.0.2`.
3. Add that commit to the top of `versions.json`, and commit as
   `plugins: release the official catalog as 0.0.2`.
4. Move `main` to the next development version, such as `0.0.3-dev.0`, in the workspace and every
   in-repository `lyra`. Set `PREVIOUS_RELEASE` in
   [`lyra-db/src/migration.rs`](../lyra-db/src/migration.rs) to the released version, empty
   `STEPS` and delete their tests, and commit as `release: start 0.0.3 development`.
5. Push the commits and the tag together.
6. In lyra-web, tag the commit that `web.rev` holds at the release with the same tag, such as
   `v0.0.2`, and push that tag. lyra-web has no tags for development versions.

CI publishes images to `registry.lyra.pub/lyra/lyra` under these tags:

| Tag | Image |
| --- | --- |
| `latest` | the newest release |
| `dev` | the newest release or development version |
| `main` | the newest commit on `main` |
| `<branch>` | the newest pipeline run by hand on that branch, with `/` replaced by `-` |
| `0.0.2`, `0.0.2-dev.1` | that version |
| `sha-<commit>` | that commit |

Tagging an older version, such as a hotfix release, moves neither `latest` nor `dev` past a newer
one. Pipelines refuse branches whose tag would be `latest`, `dev`, a version, or a `sha-` tag.

CI checks the version line, `PREVIOUS_RELEASE`, `web.rev`, and every `versions.json` entry
against the one before it with [`ci/release-checks.sh`](../ci/release-checks.sh).

The `harmony-*` crates have their own version, released when Harmony is ready rather than with
each Lyra release. Plugins depend on the server version only, so a Lyra release may ship a `-dev`
Harmony version.

## Database migrations

The database records the version and the number of migration steps that created or last migrated
it. A server opens databases from the previous release, `PREVIOUS_RELEASE` in
[`lyra-db/src/migration.rs`](../lyra-db/src/migration.rs), and from any build of its own cycle up
to its own version, running the steps in `STEPS` the database has not run yet: all of them for the
previous release, and those past its recorded count for a build of the current cycle. It refuses
newer databases, older releases, and development builds of earlier cycles, so upgrading from
`0.0.2-dev.3` to `0.0.3-dev.0` goes through `0.0.2`. While `PREVIOUS_RELEASE` is unset, before the
first release, every stamped version up to the server's own opens. CLI commands that open the
database, such as `settings reset`, refuse one the server would migrate or restamp.

Before running any step, the server copies the database and its write-ahead log beside them, named
after the stamp they hold: `lyra.db` from `0.0.3-dev.0` with one step run is copied to
`lyra.db.from-0.0.3-dev.0.steps-1` and `.lyra.db.from-0.0.3-dev.0.steps-1`, and a previous release
without a step count to `lyra.db.from-0.0.2`. Backups are never overwritten: one of the same name
with the same contents is kept as it is, and one with other contents gets a numbered sibling such
as `lyra.db.from-0.0.2.1`.

Any change to stored data, such as a renamed key, a new required value or a moved edge, needs a
step appended to `STEPS` that rewrites the shape before it into the one after. `STEPS` is
append-only: once a step is on `main`, never edit, reorder or remove it, since databases already
count it as run; fix a mistake with a new step. The steps run after the current root aliases and
indexes exist, in one transaction with them and the new stamp, so a failed step leaves the database
as it was. Test each step by seeding the shape before it directly, running it, and reading the
result through the current code.

Steps are matched by count, not identity. Local branches that share a development version and
append different steps treat each other's databases as already migrated, so recreate the database,
or restore its backup, when switching between such branches.

## Docker builds

Full image builds require a lyra-web commit SHA in `LYRA_WEB_GIT_HASH`. To build with the one in
`web.rev`, as CI does for tagged versions:

```sh
docker build \
  --build-arg LYRA_GIT_HASH="$(git rev-parse HEAD)" \
  --build-arg LYRA_WEB_GIT_HASH="$(cat web.rev)" \
  -t lyra:local .
```

To build with lyra-web's current `main` instead, as CI does for other pipelines, pass
`LYRA_WEB_GIT_HASH=$(git ls-remote https://github.com/lyra-org/lyra-web.git refs/heads/main | cut -f1)`.
