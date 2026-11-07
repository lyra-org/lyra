# Repository Guidelines

## Project Structure & Module Organization
- `lyra-server/`: main Rust server crate.
- `lyra-db/`: database crate and the primary home for database access and query logic; `lyra_db::test_db` is available to dependents' tests through its `testing` feature.
- `lyra-docs/`: generates the Luau definitions and the OpenAPI/AsyncAPI documents (`list`, `print`, `generate`, `setup`) from the renderers `lyra-server` exposes under its `docgen` feature.
- `lyra-server/src/services/`: shared business logic used by both `routes/` and `plugins/`.
- `lyra-server/src/plugins/`: plugin system integration and core capabilities such as metadata providers; follow `docs/plugin-api.md` for the Luau module surfaces.
- `lyra-server/src/routes/`: REST API route definitions and handlers; follow `docs/api.md` for path conventions.
- `lyra-server/src/config/`: startup configuration read before the database opens: `config.json`, environment variables, and storage directories. Only the `lib.rs` composition reads it, handing services the resolved values (`services/boot.rs`). Runtime settings belong to `services/settings`.
- Layers depend only downward: `lyra-db` ← `services` ← `plugins` ← `routes` ← `lib.rs` composition; `config` sits on `lyra-db` and only `lib.rs` reads it. Services reach the plugin runtime only through the abstractions they own (`services/plugin_dispatch.rs`, `services/plugin_scope.rs`), which plugins implement and publish. Each layer owns its test helpers (`services/testing.rs`, `plugins/testing.rs`) and its generation state; the plugin generation lives inside the services one (`services/state.rs`, `plugins/state.rs`), so one reset swaps both. How database and service types look from Luau belongs to the plugin layer (`plugins/types.rs` and the module that exposes them); `lyra-db` and `services` stay free of Luau.
- Do not add new top-level modules to `lyra-server/src/` — each one represents a major domain layer and should only be introduced with an explicit decision about what boundary it owns.
- Split files into subdirectory modules when they grow multiple distinct sub-responsibilities that would each benefit from independent navigation and ownership.
- `docs/`: documentation for people working on Lyra, plus the plugin repository formats. User-facing guides live on the website (https://lyra.pub), not here. Add to `docs/` only what a contributor needs and the code, the OpenAPI document, or the generated Luau definitions don't already say.
- `docs/reference/agdb.md`: agdb behavior reference (read before DB changes).
- `docs/reference/mlua.md`: mlua/mluau behavior reference (read before Lua/mlua changes).

## Backward Compatibility Policy
- Current testers expect breaking changes. Optimize for one canonical current-state implementation.
- Plugin compatibility is not guaranteed; all known plugins are maintained in this repository and updated alongside server changes.
- Databases from the previous release or from an earlier build of the current cycle must open: append a step to `STEPS` in `lyra-db/src/migration.rs` for any change to stored data, and never edit existing steps. Other databases are refused; add no other compatibility code for stored data.
- Do not introduce compatibility bridges, migration shims, fallback paths, or dual behavior for old local states beyond `lyra-db/src/migration.rs` unless explicitly requested.
- Prefer one canonical codepath, fail-fast diagnostics, and explicit recovery steps.

## Build, Test, and Development Commands
```sh
cargo build
cargo test
cargo test -p lyra-server
cargo run -p lyra-server -- serve
```
```sh
luau-lsp analyze --no-flags-enabled --flag=LuauSolverV2=true --platform standard --base-luaurc .luaurc --definitions=.lyra/defs/globals.d.luau <file1.luau> <file2.luau>
```
Run `cargo run -p lyra-docs -- setup` before Luau analysis after changing Rust-defined Luau module surfaces or generated Luau definitions.

## Coding Style & Naming Conventions
- Format Rust code with `cargo +nightly fmt` and Luau scripts with `stylua`.
- Keep comments minimal and purposeful; prefer clear code over commentary.
- Lua plugin scripts (`plugins/*.luau`) follow the existing project style, including tab-based indentation.

## Testing Guidelines
- Metadata tests rely on audio fixtures in `lyra-server/tests/assets/metadata`.
- Follow `docs/testing.md` for test vocabulary, layout, and Luau fixtures.
- Luau tests of server-provided plugin modules belong in `lyra-harmony-test/tests/luau`; a plugin's own Luau tests belong in `plugins/<plugin>/tests/luau`. Keep Rust tests for Rust-side assertions such as registry state, executor internals, or dispatch handles, and keep plugin behavior out of Rust tests.
- Keep `lyra-harmony-test`, `lyra-metadata`, and `lyra-server` behaviorally consistent. Changes to metadata parsing, lookup hints, matching, or artist resolution should be mirrored and validated across all three.

## Commit & Pull Request Guidelines
- Follow `docs/commits.md` for commit subject scopes and message style.
