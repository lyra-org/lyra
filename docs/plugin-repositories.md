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
origin, ref, and commit. Updates re-resolve the recorded ref:

- branch refs (or no ref) track new commits,
- tag and commit refs stay pinned.

Plugins without a source record are local: bundled or hand-copied. They
are never touched by repository installs, updates, or uninstalls.

## Server API and CLI

`GET /api/server/public` reports `setup.account_required` and
`setup.plugin_selection_required` instead of `setup_complete`. Plugin selection
is needed only when no plugins are installed and it has not been skipped.
`PATCH /api/server/setup` accepts `{"plugin_selection_skipped": true}` (or `false`
to clear it), requires `manage_plugins`, and returns 204.

Plugin management requires the manage-plugins permission:

- `POST /api/plugins/resolve` — preview a repository's plugins,
  including the capability scopes each plugin requests, without
  installing.
- `POST /api/plugins/install` — install all (or selected) plugins from a
  URL and reload the plugin runtime.
- `POST /api/plugins/{plugin_id}/update`, `DELETE /api/plugins/{plugin_id}`
- `GET|POST /api/plugins/repositories`,
  `POST /api/plugins/repositories/{id}/refresh`,
  `DELETE /api/plugins/repositories/{id}` — remembered repositories;
  removing one keeps its installed plugins.

From the command line:

```sh
lyra plugins add https://github.com/owner/repo
lyra plugins add https://github.com/owner/repo --ref v2
```

The CLI installs to disk only; the next server start (or an API-driven
reload) loads the plugins.
