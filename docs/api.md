# API Conventions

## Caller-Private Resources Live Under `/api/me`

A REST path says whose data it touches:

- `/api/me/*` holds data private to the authenticated caller, which the API exposes to no
  other user.
- Top-level paths hold shared resources and cross-user admin views.

| Path | Kind |
|---|---|
| `/api/me`, `/api/me/api-keys`, `/api/me/plugins` | Caller's account and settings |
| `/api/me/favorites`, `/api/me/ratings`, `/api/me/bookmarks`, `/api/me/tags`, `/api/me/playbacks` | Caller-private collections |
| `/api/me/listens` | Caller-private views |
| `/api/me/connections` | Caller's open WebSocket connections |
| `/api/releases`, `/api/tracks`, `/api/playlists`, ... | Shared resources |
| `/api/mix` | Shared tracks picked for the caller, from seeds or the caller's history |
| `/api/ratings` | Aggregates across users about shared resources, shown while `instance_stats` is on |
| `/api/users`, `/api/listens` | Cross-user admin views |

### Choosing a Path

- If only the caller can ever see or change the data, put it under `/api/me`.
- If other users can see it, keep it top-level even when it has an owner. Playlists are
  top-level because they have an `owner_id`, can be public, and admins can delete them.
- A caller's data about a shared entity stays attached to that entity, such as personal
  lyrics under `/api/tracks/{id}/...`.
- Aggregates across users about shared entities, such as average ratings, are shared data
  and take a top-level path.
- Admin views across users take the top-level path. The caller's own view of the same data
  goes under `/api/me`, as with `/api/listens` and `/api/me/listens`.
- Auth actions on the caller's session, such as `/api/users/login` and
  `/api/users/logout`, stay with the rest of the auth routes.

### Caller State in Responses

A shared resource looks the same to every caller. The caller's state about it, such as
their rating or whether they favorited it, and aggregates such as average ratings come from
batch lookups by ID (`POST /api/me/ratings/check`, `POST /api/ratings/averages`), not from
fields on the resource. `inc` only includes related resources.

Browsing by the caller's state goes through the shared list endpoints, which filter and sort
by it: `rated`, `favorite`, `listened`, `bookmarked`, `sort_by=rated_at`,
`sort_by=favorited_at` and `sort_by=bookmarked_at`, as in
`GET /api/tracks?favorite=true&sort_by=favorited_at`. `/api/me` holds no second list of the
same entities.

### Routing

- Handlers stay in their domain module (`routes/favorites.rs`, `routes/playbacks/`, ...).
  The module exposes `me_<domain>_routes()` and `me_<domain>_openapi_routes()`.
- `routes/app.rs` composes `/api/me` from those routers, keeping the axum router, the
  OpenAPI router, and `CORE_ROUTE_RESERVATIONS` in sync.
- Each endpoint has one path. Moving an endpoint removes the old path rather than leaving
  an alias.

## Previews

A preview is a POST to `<mutation path>/preview`. It takes the mutation's input, changes
nothing, and binds nothing: the mutation checks everything again when it runs. A value a
mutation compares against what the preview saw goes in the input as `expected`, which a preview
ignores.

## Metadata Change Sets

Hand curation of metadata goes through one ordered set of operations, reviewed together and
applied together:

| Path | Kind |
|---|---|
| `POST /api/metadata/changes/preview` | Reports each operation against the data as it stands |
| `POST /api/metadata/changes` | Applies the operations in order, in one transaction |

Both take `{ "changes": [ { "action": ..., ... } ] }`, holding one to 50 operations and at most 50
merge losers across them. The operations
are `edit_metadata`, `set_external_id`, `inherit_external_id`, `remove_external_id_alias`, `merge`
and `rename_genre`. Both answer `{ "results": [ ... ] }`, one entry per operation in the set's
order.

A mapping change stays out of a set: `PUT /api/metadata/mapping` reingests every library in the
background, which can't join a transaction. So do covers, lyrics and track remaps.

### Preview

A preview runs under a read lock, so it doesn't block playback, and reads each operation on its
own. It does not model the earlier operations of the set: a rename of a genre an earlier merge
would delete previews as if the genre stayed. Only the apply tells whether operations that
depend on one another hold together.

### Apply

The apply is authoritative. Just before each operation runs, it resolves the IDs the operation
names and validates it against what the operations before it staged, not against the database as
it was. An operation naming an entity an earlier one merged away reads as missing, and a rename to
a name an earlier one took is a conflict. Any failure rolls the whole set back.

### Expected Values

An operation whose meaning depends on the data it replaces carries a compact `expected`, required
when applying and compared against the staged state just before the operation runs:

| Operation | `expected` | From the preview's |
|---|---|---|
| `edit_metadata` | The field diff | `diff` |
| `set_external_id` | The record, `null` for none | `current` |
| `inherit_external_id` | The record | `current` |
| `merge` | The manual values the losers would fill | `manual_fills` |
| `rename_genre` | The genre's name | `current_name` |

The comparison comes right after the access and existence checks, ahead of whether the operation
would have an effect, so an edit someone else already made reads as stale. Aggregates such as a
merge's counts are left out, as unrelated syncs shift them. Since the
comparison sees the staged state, an operation following one that changes the same data expects
what that one leaves behind, not what the preview reported.

### Failures

A failed operation answers for the set with its own status, 400, 403, 404 or 409, and a JSON body
holding its 0-based `index` and `message`. A mismatch with `expected` is a 409 that adds `code`
`metadata_change_stale` and `current`, what a new preview would report in its place. An operation
with no effect fails the set. A failure of the set as a whole, for being empty, over a cap, or for
the caller's own permission, is a plain 400 or 403 without an `index`.

### Access and Runs

Every operation needs `ManageMetadata`, read again under the write lock, and is checked against
the staged state. `edit_metadata`, `set_external_id` and `inherit_external_id` need one reachable
library. `merge`, `rename_genre` and `remove_external_id_alias` need every library they affect. An
entity the caller reaches through no library reads as missing.

A set holding a `merge` or `rename_genre` stays off libraries a sync, refresh or purge is running
on, for every operation in it, and an operation reaching one is a 409. Any other set applies
during a run, and doesn't hold up one starting.
