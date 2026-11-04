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
the first of its chunk's `...`. The plugin goes without the scopes in `without_scopes`, which it must declare;
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
language = "jpn"     # optional ISO 639 language

[[artists]]
key = "amy"
name = "Amy"
sort_name = "Amy, The" # optional sort name, as tags give it
type = "person"      # optional artist type
manual_type = "group" # optional manually edited artist type
cover = "amy.png"    # optional image file, relative to the fixture, stored as a pinned upload whose hash is the artist key
description = "Amy's story" # optional
identifiers = { "example:artist" = "a-1" } # optional values by scheme, under a synthetic `fixture` provider

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
cover_gone = true    # optional; `cover` names a missing file, as one deleted after a sync
description = "Solo's story" # optional

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
identifiers = { "example:recording" = "r-1" } # optional values by scheme, under the `fixture` provider

[[playlists]]
key = "mix"
owner = "owner"
name = "Mix"
public = false
tracks = ["s1", "s1"]
cover = "mix.png"    # optional image file, relative to the fixture, stored as the playlist's upload whose hash is the playlist key

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

[[bookmarks]]
user = "owner"
track = "s1"         # any track key, seen by the user or not
position_ms = 42000  # stored as given, past the track's duration or not
comment = "Chorus"   # optional
at = 1700000000000   # optional creation and last change, in unix milliseconds

[[tags]]
user = "owner"
target = "solo"      # any artist, release, track or playlist key, seen by the user or not
name = "Vinyl"

[[playbacks]]
key = "phone_queue"   # optional; the playback's public id goes in `ids`
user = "owner"
tracks = ["s1", "s1"] # the queue's track keys, in order
current = 1           # optional 0-based index of the current track, 0 by default
position_ms = 30000   # optional position of the current track's play, 0 by default
state = "playing"     # optional state of that play, `paused` by default
client_name = "Phone" # optional client of that play
plugin = "subsonic"   # optional; the plugin whose client `client_name` saved the queue, which
                      # leaves the play stopped and counting no listen, and takes no `state`
updated = 1700000000000 # optional last change of the playback and its play, in unix milliseconds

[[lyrics]]
key = "fetched"      # optional; the entry's public id goes in `ids`
track = "s1"
provider = "lrclib"  # optional; a provider's entry, the provider enabled in order of first appearance
user = "owner"       # optional; the user's personal entry. Without provider or user, the shared one
language = "eng"     # optional; `und` by default
content_type = "application/lrc" # optional; as an upload names it, `text/plain` by default
body = "[00:01.00]One\n[01:40.00]Two"
```

Artist and track identifiers are scheme-level only: the `fixture` provider declares each scheme as
an id type of its own, for the one entity type it is used on, not unique and with no URL generator,
so they read as a provider's identifiers do without naming any real provider.

```luau
local fixture = ...
fixture.users.owner     -- the user's public id
fixture.libraries.music -- the library's public id
fixture.api_keys.phone  -- an API key's secret
fixture.sessions.laptop -- a session's token
fixture.connections.speaker -- a connection's public id
fixture.ids.s1          -- an entity's, library's, lyrics entry's or playback's public id, by fixture key
fixture.entries.mix[1]  -- a playlist's entry ids, in order
```

A test that needs a second user's view gets its own file and fixture with a different `run_as`.
Without a fixture, a Luau test runs as a fresh non-admin user over an empty catalog. The test's
own calls act for that user throughout.

## Luau harness

A Luau test's second `...` is a table of helpers for what a test can't reach from Luau. Its
`plugin` calls the plugin's code as the host dispatches a request: in a thread of its own, acting
for no user until the code authenticates one. A call into code that authenticates, such as an API
handler, goes through it, so whom the call authenticates doesn't become the test's user:

```luau
local fixture, harness = ...
local dispatch = require("../../dispatch")
local response = harness.plugin:call(dispatch.dispatch, ctx) -- returns what the handler returns
```

`plugin:call` isolates only the thread it creates, so a coroutine created outside it and resumed
inside keeps the test's user.

Its `task` waits whatever scopes the plugin declares, as a test polling for a run to end does:

```luau
harness.task:wait(0.05) -- yields the test for at least this many seconds
```

Its `sockets` opens and closes plugin websockets in place of a client:

```luau
local fixture, harness = ...
local sockets = harness.sockets
local sender = sockets:open() -- a sender, as an `api.websocket` handler gets it
playback_sessions.hold_session(sender, "device")
sockets:close(sender)         -- closes it as its client going away does, and returns once the
                              -- host is done with it
```
