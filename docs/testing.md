# Testing

Rust tests run with `cargo test`. Plugin behavior is tested with `lyra-harmony-test`, which runs
scenarios and Luau tests against a real server state.

## Vocabulary

| Term | Meaning |
|---|---|
| **Scenario** | A `.toml` that ingests a library of raw tags, runs it end to end through a provider (`run = "refresh"` or `"sync"`), and checks the `expect` section. |
| **Trace** | One recorded set of HTTP responses a scenario received. A scenario keeps its traces in `cache/` and must pass against each of them. |
| **Fixture** | The data a test runs against: audio assets, a scenario's ingested library, or a Luau test's `<test>.fixture.toml`. |
| **Luau test** | A `.luau` file run as the plugin that owns it, optionally with a fixture. Services it calls dispatch to the handlers it registers, as they would to a loaded plugin's. |

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
- Tests of the modules the server provides to every plugin live in `lyra-harmony-test/tests` and
  run as a placeholder plugin declaring every scope. They also run under `cargo test`.
- Files starting with `_` are helpers for other tests and never run on their own.
- `cache/` is stored with Git LFS; see [development](development.md).

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
runs as the `run_as` user, or a fresh non-admin user without one, and receives the seeded ids as
its chunk's `...`. The plugin goes without the scopes in `without_scopes`, which it must declare;
a scope another kept one narrows can't go:

```toml
run_as = "owner"
without_scopes = ["lyra.ratings"]

[[users]]
key = "owner"
admin = true

[[users]]
key = "manager"
permissions = ["manage_users"]  # a role of its own, for a non-admin

[[api_keys]]
key = "phone"
user = "owner"

[[sessions]]
key = "laptop"
user = "owner"
client_name = "Lyra Web"  # optional
seen = 300000             # optional last authenticated request, in unix milliseconds

[[connections]]           # open native WebSocket connections, for the test's length
key = "speaker"
user = "owner"
session_key = "living-room"   # optional; the fixture key without one
client_name = "Speaker"       # optional
commands = ["pause", "seek"]  # optional remote control commands it declares

[[libraries]]
key = "music"
name = "Music"
users = ["manager"]  # users granted the library; admins see every library

[[artists]]
key = "amy"
name = "Amy"
type = "person"      # optional artist type
manual_type = "group" # optional manually edited artist type

[[genres]]
key = "rock"
name = "Rock"

[[releases]]
key = "solo"
title = "Solo"
library = "music"    # optional; the release and its tracks belong to the library
added = 400000       # optional file creation time, in unix milliseconds
artists = ["amy"]
genres = ["rock"]
cover = "cover.png"  # an image file, relative to the fixture, whose hash is the release key
blurhash = "L00000fQfQfQ" # optional, the cover's blurhash

[[tracks]]
key = "s1"
title = "S1"
release = "solo"
disc = 1
track = 1
year = 2001
added = 50
duration_ms = 180000 # optional, as a scan reads it from the audio
artists = ["amy"]    # artist credits, in order
credits = [{ artist = "amy", type = "instrumentalist", detail = "piano" }]  # any credit type, after `artists`
file = "song.opus"   # an audio file, relative to the fixture, as the primary source
start_ms = 1000      # optional, with end_ms: the source's span of the file, as a cue sheet's
folder = "Amy/Solo"  # optional; the directories holding the file's entry in the release's library

[[playlists]]
key = "mix"
owner = "owner"
name = "Mix"
public = false
tracks = ["s1", "s1"]

[[listens]]
user = "owner"
track = "s1"
at = 1700000000000  # optional start, in unix milliseconds

[[favorites]]
user = "owner"
target = "mix"       # any artist, release, track or playlist key
at = 1700000000000   # optional first favorite, in unix milliseconds

[[ratings]]
user = "owner"
target = "s1"        # any artist, release or track key, seen by the user or not
value = 4            # 1 through 5

[[tags]]
user = "owner"
target = "solo"      # any artist, release, track or playlist key, seen by the user or not
name = "Vinyl"
```

```luau
local fixture = ...
fixture.users.owner     -- the user's public id
fixture.libraries.music -- the library's public id
fixture.api_keys.phone  -- an API key's secret
fixture.sessions.laptop -- a session's token
fixture.connections.speaker -- a connection's public id
fixture.ids.s1          -- an entity's or library's public id, by fixture key
fixture.entries.mix[1]  -- a playlist's entry ids, in order
```

A test that needs a second user's view gets its own file and fixture with a different `run_as`.
Without a fixture, a Luau test runs as a fresh non-admin user over an empty catalog.
