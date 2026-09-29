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
  "description": "Plugins maintained alongside the Lyra server",
  "plugins": [
    { "path": "plugins/musicbrainz" },
    { "path": "plugins/theaudiodb" },
    { "url": "https://codeberg.org/someone/lyra-foo" },
    { "url": "https://github.com/someone/lyra-bar", "ref": "v2" }
  ]
}
```

`name` is required; `description` is optional. Each entry sets exactly one of:

- `path`: a directory inside the same repository containing a
  `plugin.json` directly. Relative, forward slashes, no `..`, and no
  entry may live inside another entry's directory.
- `url`: another Git repository whose root contains a `plugin.json`. An
  optional `ref` pins a branch, tag, or commit; `path` entries cannot
  set one.

A repository root may not contain both `plugin.json` and
`repository.json`, and a `url` entry may never point at another
`repository.json` repository — references are one level deep by
construction, so indexes cannot nest or form cycles. Duplicate entries,
entries pointing at the index itself, and unknown fields are rejected.
Entries are capped at 64 and the manifest at 64 KiB.

## Repository URLs

Plain repository URLs and browser URLs both work:

- `https://github.com/owner/repo`
- `https://github.com/owner/repo/tree/develop` (ref from the URL)
- `https://gitlab.example.org/group/subgroup/repo` (nested namespaces)
- `https://codeberg.org/owner/repo/src/branch/develop`
- `https://gitlab.example.org/group/repo/-/tree/develop`

Query parameters refine resolution:

- `?ref=<branch|tag|commit>` selects a ref; use this for refs containing
  slashes, such as `release/v2`. A ref in the URL path takes precedence,
  and an explicitly requested ref overrides both.
- `?forge=github|gitlab|gitea|forgejo` overrides forge detection.
  Self-hosted hosts are treated as Gitea unless their name contains
  `gitlab`, or starts with `ghe.` or contains `.github.` for GitHub.

Only `http`/`https` URLs are supported.

## Refs, Pinning, and Updates

Without a ref, Lyra installs the repository's default branch at its
current commit. When the forge API is unreachable, Lyra tries `main`,
`master`, and `trunk` instead and records no commit, so the plugin's
update status is unknown. A branch ref tracks new commits and is re-resolved on
update; a tag or commit ref is pinned and never changes. When the forge
cannot classify a ref, it is treated as a tracking branch.

An installed plugin reports whether it is up to date, has an update
available, or has an unknown status, based on the last refresh of its
subscribed repository. Pinned plugins are always up to date.

Plugins without a repository source are local: bundled or hand-copied.
Repository installs, updates, and uninstalls never touch them. A plugin
whose source record cannot be read is reported as invalid; reinstalling
or uninstalling it repairs the record.

Removing a repository subscription keeps its installed plugins on disk.

## Managing Plugins

Plugins and repositories are managed from the server, which requires the
`manage_plugins` permission. The HTTP API is described by the OpenAPI
document, which `cargo run -p lyra-docs -- print openapi` prints.

From the command line:

```sh
lyra plugins add https://github.com/owner/repo
lyra plugins add https://github.com/owner/repo --ref v2
```

The CLI installs to disk only; the next server start or a plugin reload
loads the plugins.
