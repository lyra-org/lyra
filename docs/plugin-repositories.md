# Plugin Repositories

Lyra installs plugins from Git repositories hosted on GitHub, GitLab, or
Gitea/Forgejo (including self-hosted instances). A repository is either a
single plugin or an index of plugins.

Lyra includes its [own plugin catalog](https://git.lyra.pub/lyra/lyra?forge=gitlab)
by default. Docker images start without installed plugins. Tagged CI builds pin
the initial subscription to their release tag; existing subscriptions keep their
ref across upgrades. Removing the subscription persists across restarts.

## Single-Plugin Repository

A repository whose root contains a `plugin.json` is a single plugin. The
whole repository tree is installed as the plugin directory, named by the
manifest `id`.

## Multi-Plugin Repository

A repository whose root contains a `repository.json` is an index:

```json
{
  "schema_version": 1,
  "name": "Lyra Official Plugins",
  "description": "First-party plugins maintained alongside the server",
  "plugins": [
    { "path": "musicbrainz" },
    { "path": "metadata/theaudiodb" },
    { "url": "https://codeberg.org/someone/lyra-foo" },
    { "url": "https://github.com/someone/lyra-bar", "ref": "v2" }
  ]
}
```

Each entry sets exactly one of:

- `path`: a directory inside the same repository containing a
  `plugin.json` directly. Relative, forward slashes, no `..`, and no
  entry may live inside another entry's directory.
- `url`: another Git repository whose root contains a `plugin.json`. An
  optional `ref` pins a branch, tag, or commit.

A repository root may not contain both `plugin.json` and
`repository.json`, and a `url` entry may never point at another
`repository.json` repository — references are one level deep by
construction, so indexes cannot nest or form cycles. Entries are capped
at 64 and the manifest at 64 KiB.

## Repository URLs

Plain repository URLs and browser URLs both work:

- `https://github.com/owner/repo`
- `https://github.com/owner/repo/tree/develop` (ref from the URL)
- `https://gitlab.example.org/group/subgroup/repo` (nested namespaces)
- `https://codeberg.org/owner/repo/src/branch/develop`

Query parameters refine resolution:

- `?ref=<branch|tag|commit>` selects a ref; use this for refs containing
  slashes, such as `release/v2`.
- `?forge=github|gitlab|gitea|forgejo` overrides forge detection for
  self-hosted instances whose hostname gives no hint.

Only `http`/`https` URLs are supported.

## Refs, Pinning, and Updates

Without a ref, Lyra asks the forge API for the default branch and
resolves it to a commit, so installs are pinned to the exact tree that
was reviewed. When the API is unreachable, common branch names
(`main`, `master`, `trunk`) are probed instead and no commit is
recorded.

Installed plugins carry a `.harmony-source.json` record with their
origin, ref, commit, and whether the ref is pinned. The ref is classified
at install time: no ref or a branch tracks new commits; a tag or commit
is pinned. When the forge API cannot classify the ref, it is recorded as
tracking. Updates re-resolve tracking refs and never touch pinned ones,
which are reported as up to date without contacting the forge.

Records written by an older schema fail to load. Such a plugin is listed
with a source of kind `invalid`, updates report it as failed, and the
catalog shows it as `unknown`. Uninstalling or reinstalling it rewrites
the record; both accept a directory whose record is unreadable.

Plugins without a source record are local: bundled or hand-copied. They
are never touched by repository installs, updates, or uninstalls.

## Server API and CLI

`GET /api/server/public` reports `setup.account_required` and
`setup.plugin_selection_required` instead of `setup_complete`. Plugin selection
is needed only when no plugins are installed and it has not been skipped.
`PATCH /api/server/setup` accepts `{"plugin_selection_skipped": true}` (or `false`
to clear it), requires `manage_plugins`, and returns 204.

Plugin management requires the manage-plugins permission:

- `GET /api/plugins` — loaded plugins, each with a `source` of kind
  `repository` (origin, ref, commit, pinned, status, installed_at),
  `local`, or `invalid` (error) when the source record could not be read.
  `status` compares the installed commit with the commit stored for the
  subscribed repository of the same origin and ref, without contacting
  the forge: `up_to_date`, `update_available`, or `unknown` when no such
  subscription exists or a commit is missing on either side. Pinned
  plugins are always `up_to_date`. Refreshing the repository, or
  updating plugins, stores the latest commit.
- `POST /api/plugins/resolve` — body `{"url": ..., "ref": ...}`; preview
  a repository without installing. Returns the resolved repository shape
  described below, minus `id` and `refreshed_at`.
- `POST /api/plugins/install` — body `{"url": ..., "ref": ..., "plugins": [...]}`;
  install all (or the selected) plugins from an ad-hoc URL and reload the
  plugin runtime. An empty `plugins` list is rejected.
- `POST /api/plugins/repositories/{id}/install` — body `{}` or
  `{"plugins": [...]}`; the same, from a subscribed repository's origin
  and ref.
- `POST /api/plugins/update` — body `{}` or `{"plugins": [...]}`; updates
  the named plugins, or every repository-managed plugin when `plugins` is
  omitted. Pinned plugins are `up_to_date` without a forge call. Each
  origin is resolved once and the runtime reloads once; the response lists
  `updated`, `up_to_date`, and `failed` plugins.
- `POST /api/plugins/reload` — reload the plugin runtime from disk, for
  example after a CLI install or a failed reload. Returns 204.
- `DELETE /api/plugins/{plugin_id}` — uninstall a repository-managed plugin.
- `GET /api/plugins/repositories` — subscribed repositories as remembered
  by the server: `{id, origin, name, description, ref?, commit?, refreshed_at?}`;
  `refreshed_at` appears once the repository has been refreshed.
- `POST /api/plugins/repositories` (body `{"url": ..., "ref": ...}`) and
  `POST /api/plugins/repositories/{id}/refresh` — subscribe to, or
  re-resolve, a repository. Both return the resolved repository.
- `DELETE /api/plugins/repositories/{id}` — forget a subscription; its
  installed plugins stay on disk.

A resolved repository looks like:

```json
{
  "id": "…",
  "origin": "https://github.com/owner/repo",
  "name": "Lyra Official Plugins",
  "description": "…",
  "ref": "v2",
  "resolved_ref": "v2",
  "commit": "…",
  "refreshed_at": "2026-01-01T00:00:00Z",
  "plugins": [
    {
      "id": "musicbrainz",
      "name": "MusicBrainz",
      "version": "1.0.0",
      "description": "…",
      "scopes": ["metadata"],
      "commit": "…",
      "status": "update_available",
      "source": { "origin": "https://codeberg.org/someone/lyra-foo" }
    }
  ]
}
```

`ref` is the subscribed or requested ref and is absent when none was
given; `resolved_ref` is what it resolved to. `name` and `description`
come from `repository.json` when present, else the repository name and an
empty string. Each plugin's `status` is one of `available` (not
installed), `up_to_date`, `update_available`, `unknown` (installed from a
repository but no commit recorded on one side), or `local` (installed
without a source record). `source` is present only when a `url` entry
points at another repository.

From the command line:

```sh
lyra plugins add https://github.com/owner/repo
lyra plugins add https://github.com/owner/repo --ref v2
```

The CLI installs to disk only; the next server start (or an API-driven
reload) loads the plugins.
