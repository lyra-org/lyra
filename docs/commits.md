# Commit Convention

Lyra commits use scoped, imperative subjects:

```text
<scope>[/<subscope>]: <imperative subject>
```

Examples:

```text
server/tags: add target read APIs
server/plugins: reload manifests on restart
server/services/metadata: tighten artist resolution
plugins/jellyfin: wire tags into item responses
plugins/musicbrainz: sanitize fixture paths
harmony/globals: move plugin globals into shared crate
metadata: parse replaygain tags
docs: document commit convention
```

## Subject Rules

- Use lowercase scopes.
- Use an imperative subject, such as `add`, `fix`, `remove`, `rename`, `wire`, `tighten`, or `document`.
- Keep the subject focused on the user-visible or maintainer-visible change.
- Do not end the subject with a period.
- Prefer one logical change per commit.
- Use a body when the reason, tradeoff, recovery step, or compatibility impact is not obvious from the subject.

## Scope Selection

- Prefer the most specific ownership scope that describes the change.
- Use `server/<layer>` when a change is isolated to a server layer such as DB, routes, services, or plugin bindings.
- Use `server/<domain>` when one server domain crosses layers, such as tags, metadata, libraries, playback, or covers.
- Use `plugins/<name>` when a change belongs to one bundled plugin.
- Use `harmony/<name>` for the shared Harmony crates.
- Use package scopes such as `metadata`, `ffmpeg`, and `chromaprint` for standalone Lyra support crates.
- Use infrastructure scopes such as `build`, `ci`, `docker`, `deps`, and `release` only when the change is not owned by a product domain.
- Avoid vague scopes such as `misc`, `chore`, `cleanup`, or `changes`.
- Split unrelated domains into separate commits instead of using a broad scope.

## Canonical Scopes

Server scopes:

```text
server
server/auth
server/cors
server/covers
server/db
server/hls
server/libraries
server/metadata
server/playback
server/plugins
server/providers
server/routes
server/services
server/tags
```

Bundled plugin scopes:

```text
plugins
plugins/audiomuse
plugins/jellyfin
plugins/listenbrainz
plugins/musicbrainz
plugins/wikidata
```

Lyra support crate scopes:

```text
chromaprint
ffmpeg
metadata
```

Harmony crate scopes:

```text
harmony
harmony/core
harmony/crypt
harmony/globals
harmony/http
harmony/json
harmony/luau
harmony/macros
harmony/net
harmony/task
harmony/test
```

Repository scopes:

```text
build
ci
deps
docker
docs
fixtures
release
tests
```

## Commit Bodies

Use a body when it helps future maintainers understand the change. Good body topics include:

- Why a behavior changed.
- What recovery step is expected after a breaking local-state change.
- Which plugin or API surface changed.
- Why a less obvious implementation was chosen.
- Which validation was run when the subject is risky.

Keep the subject as the summary. The body should explain context, not repeat the subject.
