# Plugin Repositories

Lyra installs plugins from Git repositories hosted on GitHub, GitLab, or
Gitea/Forgejo (including self-hosted instances). A repository is either a
single plugin or an index of plugins.

Lyra includes its [own plugin catalog](https://git.lyra.pub/lyra/lyra?forge=gitlab)
by default. Docker images start without installed plugins. Removing the
subscription persists across restarts.

## Plugin Manifest

Every plugin directory contains a `plugin.json`:

```json
{
  "schema_version": 1,
  "id": "musicbrainz",
  "name": "MusicBrainz Provider",
  "version": "0.4.0",
  "lyra": "0.0.1",
  "description": "Matches local albums to MusicBrainz releases",
  "entrypoint": "init.luau",
  "scopes": ["harmony.http", "lyra.metadata"],
  "dependencies": {
    "shared-helper": "0.2",
    "fanart": { "version": "0.1", "optional": true }
  }
}
```

`version` is the plugin's own [semantic version](https://semver.org/).
`lyra` is a Cargo-style version requirement on the server: a bare version
is a caret requirement, so `"0.0.1"` accepts only 0.0.1 and `"0.3"` accepts
0.3.0 up to, but excluding, 0.4.0. A plugin whose requirement excludes the
running server fails to load.

`dependencies` maps plugin ids to version requirements in the same syntax,
either directly or as an object with a required `version` and an optional
`optional` flag. A plugin fails to load when a required dependency is
missing, or when any installed dependency, optional or not, has a version
outside its requirement. Installed dependencies load first, and a plugin can
only `require` plugins it lists that loaded. Each id may appear once, and up
to 32 dependencies are allowed.

## Releases

A repository is installable only through the releases listed in the
`versions.json` at its root, newest first:

```json
[
  { "commit": "9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b", "lyra": "0.0.2" },
  { "commit": "3f1c2a9e8b7d6c5f4e3a2b1c0d9e8f7a6b5c4d3e", "lyra": "0.0.1" }
]
```

`commit` is the full 40-character commit id of a release, each listed once,
and `lyra` the server versions it is offered to, in the same syntax as
`plugin.json`. Unknown fields are rejected, and the file is capped at
64 KiB. An entry may point at any commit the forge has.

Lyra reads `versions.json` at the subscribed ref and takes the first entry
whose `lyra` accepts the running server; everything else is read at that
release commit, never at the branch tip: the root `plugin.json` or
`repository.json`, the index's entries, and each plugin's `plugin.json`.
Plugin versions live only in `plugin.json`. A repository without a
`versions.json`, or without a release for the running server, has nothing
installable. A pre-release server such as 0.0.6-dev matches only
requirements that name a pre-release of 0.0.6, such as `">=0.0.6-dev"`;
`"*"` does not match it.

`versions.json` decides what is offered, and each `plugin.json` at the
release commit decides what loads: it goes through every check above, its
own `lyra` included, and previews show its name, description, and scopes.
An entry can narrow the servers a release is offered to, but never widen
them. To withdraw a release, remove its entry.

To make a release:

1. Bump `version`, and `lyra` when the supported servers change, in every
   plugin whose files changed since the previous release.
2. Commit.
3. In a follow-up commit, add an entry with that commit's id at the top of
   `versions.json`.

To ship a hotfix while the default branch holds unfinished work, release a
commit on a branch cut from the last release commit.

All plugins of a repository are released together and share their
release's `lyra` entry; a plugin that fails at the release commit does not
fall back to an older release.

Previews list a plugin without an installable release with the reason,
and installing or updating it fails on its own without affecting the
repository's other plugins, as do plugins sharing an id. Without a release,
plugins are named by the `plugin.json` files at the subscribed ref. A
`plugin.json` without a readable, valid `id` fails the whole repository.

## Single-Plugin Repository

A repository whose root contains a `plugin.json` is a single plugin. The
whole repository tree at the release commit is installed as the plugin
directory, named by the manifest `id`.

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
  `plugin.json` directly, released with the index. Relative, forward
  slashes, no `..`, and no entry may live inside another entry's
  directory.
- `url`: another Git repository whose root contains a `plugin.json`,
  released by its own root `versions.json` while the index has a release
  for the running server; otherwise it carries the index's reason. An
  optional `ref` names the branch, tag, or commit that file is read at;
  `path` entries cannot set one.

A repository root may not contain both `plugin.json` and
`repository.json`, and a `url` entry may never point at another
`repository.json` repository — references are one level deep by
construction, so indexes cannot nest or form cycles. Duplicate entries,
entries pointing at the index itself, and unknown fields are rejected.
Entries are capped at 64 and the manifest at 64 KiB.

## Excluding Files

Lyra downloads the forge's archive of the repository, so paths marked
`export-ignore` in `.gitattributes` are neither downloaded nor installed.
Mark test fixtures and other development-only files this way:

```gitattributes
tests export-ignore
```

This repository excludes `plugins/*/tests`.

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

Without a ref, Lyra reads the releases at the repository's default branch.
Lyra resolves refs with one Git protocol v2 ref listing
from the repository's Git endpoint, and through the forge API when the
forge does not answer it; abbreviated commits and revision expressions
such as `main~1` always go through the API. Full ref names such as
`refs/heads/main` work too; only `refs/heads/` names track.
When neither is reachable, Lyra tries `main`, `master`, and `trunk`
instead, downloading the branch's archive to read its files.
When the forge rate-limits any of these requests, or a download, the
operation stops and reports how long the forge asked to wait; it never
falls back to other requests. Update checks report the affected plugins
as failed rather than as having an update. Once a host rate-limits an
update check, plugins from its other repositories are reported as failed
too, without being checked.
A tag and a branch with the same name resolve to the tag's commit, but
the ref tracks like a branch. A branch ref tracks new releases and is
re-read on update; a tag or commit ref is pinned and never changes. When
the forge cannot classify a ref, it is treated as a tracking branch.

An installed plugin has an update available when its subscribed
repository, at its last refresh or update check, offered a higher version
for the running server. It is incompatible when the repository offers no
release of it that runs on the server: no `versions.json` entry accepts
the server, or the plugin's `plugin.json` at the release does not; the
status names the requirement that excludes the server, and updating the
plugin reports the same without downloading anything. It is up to date
when the offered release is not newer. Its status is unknown when no
subscription with the same origin and ref lists it, or when the repository
has no offer for it for another reason, such as a missing `versions.json`;
updating it then fails with the reason. Pinned plugins are always up to
date.

Plugins without a repository source are local: bundled or hand-copied.
Repository installs, updates, and uninstalls never touch them. A plugin
whose source record cannot be read is reported as invalid; reinstalling
or uninstalling it repairs the record.

Removing a repository subscription keeps its installed plugins on disk.

## Managing Plugins

Plugins and repositories are managed from the server, which requires the
`manage_plugins` permission. The HTTP API is described by the OpenAPI
document, which `cargo run -p lyra-docs -- print openapi` prints. An
install names each plugin with the release commit its preview showed, so
the scopes approved are the scopes granted: a plugin whose offered release
has changed since fails to install.

From the command line:

```sh
lyra plugins add https://github.com/owner/repo
lyra plugins add https://github.com/owner/repo --ref v2
```

The CLI installs every plugin at the release offered now, prints each
one's version and scopes, and installs to disk only; the next server start
or a plugin reload loads the plugins.
