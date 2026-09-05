# Configuration

Lyra works with its default settings. You can skip this page unless you want to change them.

## Use a configuration file

Create `config.json` beside `compose.yaml`. For example, to scan for new music every five minutes:

```json
{
  "sync": {
    "interval_secs": 300
  }
}
```

Add this line under `volumes` in the `lyra` service:

```yaml
      - ./config.json:/lyra/config.json:ro
```

Run `docker compose up -d` to apply it. After editing the file later, run `docker compose restart`.

Settings in this file override saved settings. Remove a setting from the file to allow it to be changed through the API again.

## Reset saved settings

If Lyra cannot start because a saved setting is invalid, run:

```sh
docker compose run --rm lyra settings reset
```

This clears all saved server settings. It does not remove values from `config.json`. Start Lyra again with `docker compose up -d`.

## Advanced reference

The details below are for custom deployments and API clients.

### File loading and defaults

- Outside Docker, Lyra searches for `config.json` in the working directory, beside the binary, and in the source tree. `LYRA_CONFIG_PATH` selects an exact file; it must exist.
- Unknown keys and invalid values prevent startup. The error identifies the problem.
- `port`, `db`, and `library` are startup options. Other settings use the file value first, then the saved value, then the default.
- In the file, `null` explicitly unsets `published_url` or `hls.temp_disk_budget_bytes`. Other settings reject `null`.
- Most API changes apply immediately. `rate_limit.*` and `hls.cleanup_startup_purge` require a restart.

### Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `LYRA_CONFIG_PATH` | searched | Explicit path to `config.json`; must exist when set |
| `LYRA_DATA_DIR` | `./data` | Root for server-owned state; created when serving |
| `LYRA_DB_DIR` | data dir | Directory for relative `db.path` values; created when serving |
| `LYRA_PORT` | `4746` | Listening port; overrides `port` from the file |
| `LYRA_PLUGINS_DIR` | `./plugins` | Directory plugins are loaded from |
| `LYRA_STATIC_DIR` | searched | Directory for static web assets |

Docker defaults to `/lyra/data`, `/lyra/plugins`, and `/lyra/static` for data, plugins, and web files. If you change one of these paths, update its volume mount too. `LYRA_PORT` overrides the file’s `port` value.

### Settings API

These endpoints require `manage_server` permission. Writes also require a session token; API keys allow reads only.

| Request | Effect |
| --- | --- |
| `GET /api/server/settings` | Read settings, defaults, sources, file locks, and pending restarts. |
| `PATCH /api/server/settings` | Save values, for example `{"values":{"sync.interval_secs":300}}`. Use `null` to reset a saved value to its default. |
| `DELETE /api/server/settings` | Clear all saved settings. |

All three return the settings view. It includes `boot` for active startup paths and port, and `pending_restart` for changes awaiting restart. PATCH rejects invalid values or unknown keys with `400`, and file-locked settings with `409`; rejected requests write nothing.

### Full configuration example

Copy [`config.example.json`](../config.example.json) to `config.json`:

```sh
cp config.example.json config.json
```

Keep only the settings you want to control from the file; omitted settings use their saved values or defaults.

- `published_url` accepts a public HTTP or HTTPS origin, such as `https://music.example.com`.
- `covers_path` is relative to the data directory. `db.path` is relative to `LYRA_DB_DIR`, or the data directory when unset.
- `db.kind` accepts `mmap`, `file`, or `memory`. `memory` uses a temporary database.
- Durations are in seconds. `sync.interval_secs: 0` disables periodic scans.
- HLS disk budgets are in bytes; `null` or `0` means no budget. `max_concurrent_transcodes: 0` means no limit.

For development only, you can add a `library` block to create and scan a library at startup. Normally, [add music through the API](installation.md#2-add-your-music).

```json
{
  "library": {
    "path": "/music",
    "name": "Music",
    "language": "en",
    "country": "US"
  }
}
```

Use a path visible to the server. `language` and `country` are optional; language accepts ISO 639 codes or names, and country accepts country codes or names.
