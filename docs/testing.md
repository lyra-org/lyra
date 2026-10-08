# Testing

Rust tests run with `cargo test`. Plugin behavior is tested with `lyra-harmony-test`, which runs
scenarios and Luau tests against a real server state.

## Vocabulary

| Term | Meaning |
|---|---|
| **Scenario** | A `.toml` that ingests a library of raw tags, runs it end to end through a provider (`run = "refresh"` or `"sync"`), and checks the `expect` section. |
| **Trace** | One recorded set of HTTP responses a scenario received. A scenario keeps its traces in `cache/` and must pass against each of them. |
| **Fixture** | The data a test runs against: audio assets, a scenario's ingested library, or a Luau test's `<test>.fixture.toml`. |
| **Luau test** | A `.luau` file run as the plugin that owns it, optionally with a fixture. |

## Layout

A test directory holds the scenarios at its top level and the Luau tests under `luau/`:

```text
plugins/<plugin>/tests/
  <scenario>.toml
  cache/              traces, written by --discover
  luau/
    <test>.luau
    <test>.fixture.toml
```

- A plugin's own tests live in `plugins/<plugin>/tests`.
- Tests of the modules the server provides to every plugin live in `lyra-harmony-test/tests`,
  under a placeholder plugin manifest. They also run under `cargo test`.
- Files starting with `_` are helpers for other tests and never run on their own.

## Running

```sh
cargo run -p lyra-harmony-test -- plugins/musicbrainz/tests
cargo run -p lyra-harmony-test -- --filter <name> plugins/musicbrainz/tests
cargo run -p lyra-harmony-test -- plugins/jellyfin/tests/luau/listing.luau
```

A directory runs every test in it; a single file runs as a scenario or a Luau test by its extension.
Scenarios replay their stored traces offline. `--discover` runs them against the live providers
and stores any new trace, `--prune` removes traces and responses no scenario uses, and `--record`
writes what a passing scenario captured into its `expect` section.

## Luau fixtures

A `<test>.fixture.toml` beside a Luau test seeds a fresh database before the test runs. The test
runs as the `run_as` user and receives the seeded ids as its chunk's `...`:

```toml
run_as = "owner"

[[users]]
key = "owner"
admin = true

[[artists]]
key = "amy"
name = "Amy"
type = "person"      # optional artist type

[[genres]]
key = "rock"
name = "Rock"

[[releases]]
key = "solo"
title = "Solo"
added = 400          # ctime
artists = ["amy"]
genres = ["rock"]

[[tracks]]
key = "s1"
title = "S1"
release = "solo"
disc = 1
track = 1
year = 2001
added = 50
artists = ["amy"]    # artist credits, in order
credits = [{ artist = "amy", type = "instrumentalist", detail = "piano" }]  # any credit type, after `artists`

[[playlists]]
key = "mix"
owner = "owner"
name = "Mix"
public = false
tracks = ["s1", "s1"]

[[listens]]
user = "owner"
track = "s1"

[[favorites]]
user = "owner"
target = "mix"       # any artist, release, track or playlist key
```

```luau
local fixture = ...
fixture.users.owner     -- the user's public id
fixture.ids.s1          -- an entity's database id, by fixture key
fixture.entries.mix[1]  -- a playlist's entry ids, in order
```

A test that needs a second user's view gets its own file and fixture with a different `run_as`.
Without a fixture, a Luau test runs as a fresh non-admin user over an empty catalog.
