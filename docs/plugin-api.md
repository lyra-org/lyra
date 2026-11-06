# Plugin API Conventions

These conventions cover the Luau modules the server exposes to plugins (`lyra/*`). Older
modules predate some of them.

## Ids

- Plugins see only public ids, as strings: in arguments, in record `id` fields, as map
  keys, and in callback payloads. Database ids stay inside the server, since agdb reuses
  them after deletions.
- An id is named for what it identifies, such as `track_id` and `user_id`.

## Ids in Provider Contexts

- Every handler context carries an entity's ids as `external_ids`, keyed by scheme
  (`musicbrainz:release`, `isrc`): every provider's non-empty values, a user-set value in place of
  a provider's. A provider reads its own ids under the schemes it registered, and a scheme belongs
  to the one provider that registers it, so no other value stands in for its own.
- Refresh contexts also carry `unmatched`, the schemes whose provider searched and found no match,
  and `user_set`, the schemes whose value the user set. Both are sorted arrays of schemes. Nested
  entities carry the same fields as the entity refreshed.
- A `require` path names an id as `external_ids.<scheme>`, such as
  `external_ids.musicbrainz:release`.

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

## Error Kinds

- Plugins branch on what a host error means through `errors.kind(err)` from `@lyra/errors`,
  never on its message, which is for people and may change.
- A kinded error is a userdata whose `tostring` is its message. Other host errors stay strings,
  and `errors.kind` returns nil for anything that isn't a kinded error.
- The kinds are `not_found`, `forbidden` (the caller may not do it), `unauthenticated` (the
  credential the call acts for no longer verifies) and `conflict` (the target's state refuses the
  change).
- A kind joins the list only when it means the same for any module, with nothing specific to one
  function, and only once a plugin branches on it.

## Other Users' Data

- A function that reads or affects every user's data needs its module's `.all` scope as
  well, such as `lyra.listens.all` for `listens.on_update`.
- A read that ignores the caller's visibility is a separate function named for what it
  serves, documented as being for that alone, as `tracks.streamable` and `covers.artwork`
  are for public routes.

## Plugin Data

- A `@lyra/datastore` store belongs to the plugin whose code opens it. The host takes that plugin
  from the code calling `get_or_create`, so plugins using the same name get separate stores, and a
  store name needs no plugin prefix.
- Code keeps its plugin wherever it runs: a dependency's module opens the dependency's stores when
  another plugin calls it, and a callback opens its own plugin's stores when another plugin calls
  it back.
- No function opens another plugin's store. A plugin that needs another's data lists it under
  `dependencies`, requires it and calls what it exports.
- A plugin exports functions over its stores, never a `DataStore` handle. The host checks who opens
  a store but not who calls a handle's methods, so a handle passed to another plugin gives it full
  access to that store.

## Library Changes

- A catalog module's scope only reads the library and carries no effects. Its functions
  that change the library need the module's `.manage` scope as well, such as
  `lyra.genres.manage` for `genres.resolve` and `lyra.releases.manage` for
  `releases.replace_chains`. `lyra.metadata` is the exception: it registers metadata
  providers, which change the library through the provider's methods, so the scope itself
  carries that effect.
- Removing a record whose file is confirmed gone counts as reading, not changing the
  library: `covers.get` and `covers.artwork` with `require_file` do so under `lyra.covers`,
  as the public cover route does for anyone.

## Privileged Actions

- A privileged action with effects beyond its module's needs a scope of its own within the
  module's, named for the action, such as `lyra.libraries.manage` for `libraries.sync` and
  `lyra.metadata.refresh` for `metadata.refresh`.

## Arguments and Results

- Optional arguments go in a trailing options table, with each field's default
  documented, rather than in trailing positional parameters.
- Queries take `offset`, `limit` and `sort` (`{ key, order }`) and return a page of
  `{ items, total, offset }`.
- A summary's counts and totals follow the query's scope filters, the library and the related
  items it narrows to, as `artists.query` with `library_id` counts only that library's releases.
  Filters that only choose which items appear, such as a credit role, leave them alone.
- A range filter takes `min_<field>` and `max_<field>`, both inclusive and either optional, and
  raises when the minimum exceeds the maximum, as `min_rating` and `min_year` do.
- A lookup by exact name goes through the catalog queries' `name` filter, which compares names as
  search does, ignoring case, accents, apostrophes and punctuation, rather than byte for byte. A
  plugin doesn't search and compare names itself.
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
- Stored data that expires takes its lifetime as a `ttl_ms` option on the write, as
  `DataStore:set` does, rather than an expiry field the plugin checks. The host computes the
  expiry, reads treat expired entries as absent, and a write without `ttl_ms` stores a
  persistent value.

## Documentation

- A function's doc says what it returns, what a map is keyed by, and what is left out,
  including what the caller cannot see.
- Arguments go undocumented when their types already say enough.
- Record fields are documented inline with `--`, including their defaults.
