# Configuration

Configuration is optional.

## Use a configuration file

Create `config.json` beside `compose.yaml` before mounting it; otherwise Docker creates a directory in its place. For example, to refresh metadata from providers every hour:

```json
{
  "sync": {
    "interval_secs": 3600
  }
}
```

Add this line under `volumes` in the `lyra` service:

```yaml
      - ./config.json:/config.json:ro
```

Run `docker compose up -d` to apply it. After editing the file later, run `docker compose restart`.

Settings in this file override saved settings. Remove a setting from the file to allow it to be changed from Lyra again.

## Reset saved settings

If Lyra cannot start because a saved setting is invalid, stop it and reset its settings:

```sh
docker compose stop
docker compose run --rm lyra settings reset
```

This clears all saved server settings. It does not remove values from `config.json`. Start Lyra again with `docker compose up -d`.

## Advanced reference

### File loading and defaults

- Lyra searches for `config.json` in the working directory, beside the binary, and in the binary's parent directory. `LYRA_CONFIG_PATH` selects an exact file; it must exist.
- Unknown keys and invalid values prevent startup. The error identifies the problem.
- `port` and `db` are startup options. Other settings use the file value first, then the saved value, then the default.
- In the file, `null` explicitly unsets `published_url` or `hls.temp_disk_budget_bytes`. Other settings reject `null`, except startup options, which treat it as omitted.
- Most setting changes apply immediately. `rate_limit.*` and `hls.cleanup_startup_purge` require a restart.

### Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `LYRA_CONFIG_PATH` | searched | Explicit path to `config.json`; must exist when set |
| `LYRA_DATA_DIR` | `./data` | Root for server-owned state; created when serving |
| `LYRA_DB_DIR` | data dir | Directory for relative `db.path` values; created when serving |
| `LYRA_PORT` | `4746` | Listening port; overrides `port` from the file |
| `LYRA_PLUGINS_DIR` | `./plugins` | Directory plugins are loaded from; created when serving |
| `LYRA_STATIC_DIR` | searched | Directory for static web assets; must be a directory when set |
| `RUST_LOG` | `lyra_server=info,tower_http=debug,harmony_core=info` | Log filter |

Docker uses `/data` and `/plugins`; update their mounts if you change these paths. For a frontend override, see [custom web interface](installation.md#optional-use-a-custom-web-interface).

### Full configuration example

Copy [`config.example.json`](../config.example.json) to `config.json`, keeping only your overrides.

- `published_url` accepts a public HTTP or HTTPS origin, such as `https://music.example.com`.
- `covers_path` is relative to the data directory. `db.path` is relative to `LYRA_DB_DIR`, or the data directory when unset.
- `db.kind` accepts `mmap`, `file`, or `memory`. `memory` never writes the database to disk.
- Durations are in seconds. `sync.interval_secs` sets how often provider metadata is refreshed; `0` disables periodic refreshes, but one still runs at startup.
- The HLS disk budget is in bytes; `null` or `0` means no budget. `max_concurrent_transcodes: 0` means no limit.
