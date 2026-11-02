# Plugin API Conventions

These conventions cover the Luau modules the server exposes to plugins (`lyra/*`). Older
modules predate some of them.

## Ids

- Plugins see only public ids, as strings: in arguments, in record `id` fields, as map
  keys, and in callback payloads. Database ids stay inside the server, since agdb reuses
  them after deletions.
- An id is named for what it identifies, such as `track_id` and `user_id`.

## Reading Many Items

- A read takes a list of ids and returns a map keyed by id, as `tracks.get`,
  `ratings.get` and `covers.get` do.
- A read grouped by a parent is named `by_<parent>`, takes the parent ids, and returns a
  list per parent id, as `tracks.by_release` and `entries.by_track` do.
- A read returns the value itself, not a flag. `favorites.get` returns favorite times
  rather than whether each target is a favorite, so there is no `has`/`has_many` pair.

## Limits

- Reads by id take any number of ids.
- A write applied in one transaction states its limit and raises past it, applying
  nothing, as `listens.submit` does.
- An input that drives expensive work states its limit and raises past it, as mix seeds
  do.
- A result size the caller asks for is capped rather than refused, as a mix `limit` is.
- A stated limit is exported as a module field named `MAX_<WHAT>` in upper snake case, such
  as `playlists.MAX_TRACK_IDS`, so callers check against it instead of copying the number.
  Its value is the one the host enforces.

## Missing, Hidden and Invalid

- Targets that are missing, or hidden from the caller, are absent from returned maps. A
  single-target call returns `false` or `nil` for them.
- Invalid input raises an error, such as a rating outside 1-5.

## Other Users' Data

- A function that reads or affects every user's data needs its module's `.all` scope as
  well, such as `lyra.listens.all` for `listens.on_update`.
- A read that ignores the caller's visibility is a separate function named for what it
  serves, documented as being for that alone, as `tracks.streamable` and `covers.artwork`
  are for public routes.

## Privileged Actions

- A privileged action with effects beyond its module's needs a scope of its own within the
  module's, named for the action, such as `lyra.libraries.manage` for `libraries.sync` and
  `lyra.metadata.refresh` for `metadata.refresh`.

## Arguments and Results

- Optional arguments go in a trailing options table, with each field's default
  documented, rather than in trailing positional parameters.
- Queries take `offset`, `limit` and `sort` (`{ key, order }`) and return a page of
  `{ items, total, offset }`.
- A result that comes in several forms is a union tagged by `kind`, as API responses and
  provider results are.
- Quantities name their unit: `duration_ms`, `timeout_ms`, `bitrate_bps`,
  `sample_rate_hz`.

## Enumerated Values

- Enumerated values are string unions in both directions, spelled as they serialize:
  `snake_case`, unless an external standard fixes the spelling, as `"GET"` is for HTTP
  methods.
- Rust rejects an unknown value with an error that names the accepted ones.
- Modules export no enum tables or constants tables; a limit is a single `MAX_<WHAT>`
  field.
- A value with behaviour is a class, not an enum.
- Code that branches over every value of a union ends with `local _: never = value`, so
  a misspelled or missing branch fails analysis:

  ```luau
  if role == "track" then
      ...
  elseif role == "release" then
      ...
  elseif role == "any" then
      ...
  else
      local _: never = role
  end
  ```

## Times

- Points in time are Unix milliseconds, including ones without a name, such as the
  values `favorites.get` returns.
- A field holding a point in time is named `*_at_ms`, such as `created_at_ms` and
  `started_at_ms`.
- Sort keys name an ordering, not a value, so they take no unit and match the REST
  `sort_by` keys, such as `favorited_at` and `last_played_at`.

## Documentation

- A function's doc says what it returns, what a map is keyed by, and what is left out,
  including what the caller cannot see.
- Arguments go undocumented when their types already say enough.
- Record fields are documented inline with `--`, including their defaults.
