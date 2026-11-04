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
