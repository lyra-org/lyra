# API Conventions

These are the conventions REST endpoints follow: where a path goes, how requests and responses
are shaped, and how routes are composed. What an endpoint does, including its fields, limits,
permissions and errors, belongs in its OpenAPI description, not here.

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
| `/api/mix` | Shared tracks picked for the caller |
| `/api/ratings` | Aggregates across users about shared resources |
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
by it, as in `GET /api/tracks?favorite=true&sort_by=favorited_at`. `/api/me` holds no second
list of the same entities.

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
ignores. A mismatch is a 409 with a `code` and `current`, what a new preview would report in place
of `expected`. `POST /api/users/device-logins/approve/preview` is the exception to changing
nothing: a code it doesn't find counts against the caller's limit on guessing codes.

## Metadata Change Sets

Hand curation of metadata goes through one ordered set of operations, `POST /api/metadata/changes`
and its preview. A new curation operation joins the set rather than taking a mutation path of its
own. Work that can't join a transaction, such as a mapping change's background reingest, keeps its
own endpoint, as do covers, lyrics and track remaps.
