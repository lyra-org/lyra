# Repository Guidelines

## Project Structure & Module Organization
- `lyra-server/`: main Rust server crate.
- `lyra-server/src/db/`: primary home for database access and query logic.
- `lyra-server/src/services/`: shared business logic used by both `routes/` and `plugins/`.
- `lyra-server/src/plugins/`: plugin system integration and core capabilities such as metadata providers.
- `lyra-server/src/routes/`: REST API route definitions and handlers.
- Do not add new top-level modules to `lyra-server/src/` — each one represents a major domain layer and should only be introduced with an explicit decision about what boundary it owns.
- Split files into subdirectory modules when they grow multiple distinct sub-responsibilities that would each benefit from independent navigation and ownership.
- `docs/reference/agdb.md`: agdb behavior reference (read before DB changes).
- `docs/reference/mlua.md`: mlua/mluau behavior reference (read before Lua/mlua changes).

## Backward Compatibility Policy
- Current testers expect breaking changes. Optimize for one canonical current-state implementation.
- Plugin compatibility is not guaranteed; all known plugins are maintained in this repository and updated alongside server changes.
- Data migrations are not required; breaking existing stored data is acceptable.
- Do not introduce compatibility bridges, migration shims, fallback paths, or dual behavior for old local states unless explicitly requested.
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
Run `cargo run -p lyra-server -- docs setup` before Luau analysis after changing Rust-defined Luau module surfaces or generated Luau definitions.

## Coding Style & Naming Conventions
- Format Rust code with `cargo fmt` and Luau scripts with `stylua`.
- Keep comments minimal and purposeful; prefer clear code over commentary.
- Lua plugin scripts (`plugins/*.luau`) follow the existing project style, including tab-based indentation.

## Testing Guidelines
- Metadata tests rely on audio fixtures in `lyra-server/tests/assets/metadata`.
- Keep `lyra-harmony-test`, `lyra-metadata`, and `lyra-server` behaviorally consistent. Changes to metadata parsing, lookup hints, matching, or artist resolution should be mirrored and validated across all three.

## Commit & Pull Request Guidelines
- Follow `docs/commits.md` for commit subject scopes and message style.
