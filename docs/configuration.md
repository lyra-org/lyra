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
| `RUST_LOG` | `lyra_server=info,tower_http=debug,harmony_core=info` | Log filter; request logs name query parameters without their values, which log at `lyra::http::query=trace` only when that target is named |

Docker uses `/data` and `/plugins`; update their mounts if you change these paths. For a frontend override, see [custom web interface](installation.md#optional-use-a-custom-web-interface).

### Full configuration example

Copy [`config.example.json`](../config.example.json) to `config.json`, keeping only your overrides.

- `published_url` accepts a public HTTP or HTTPS origin, such as `https://music.example.com`.
- `covers_path` is relative to the data directory. `db.path` is relative to `LYRA_DB_DIR`, or the data directory when unset.
- `instance_stats` shows users averages computed across all users, such as average ratings. When off, genre covers also follow only the user's own listens.
- `db.kind` accepts `mmap`, `file`, or `memory`. `memory` never writes the database to disk.
- Durations are in seconds. `sync.interval_secs` sets how often provider metadata is refreshed; `0` disables periodic refreshes, but one still runs at startup.
- The HLS disk budget is in bytes; `null` or `0` means no budget.
- `plugins.auto_update` installs plugin updates the daily check finds unless they need approval; see [automatic updates](plugin-repositories.md#automatic-updates).
- Transcoded audio is cached briefly in `transcodes` under the data directory, which is emptied at startup. `transcoding.cache_max_bytes` bounds its size (a larger transcode is still served, just not kept) and `transcoding.cache_ttl_seconds` removes transcodes not requested for that long. No new transcode starts while that disk has less than 1 GiB free; such requests get `503 Service Unavailable` with `Retry-After`.
- Resized or converted covers are kept in `cover-transforms` under the data directory; `covers.transform_cache_max_bytes` bounds its size (512 MiB by default), oldest evicted first. A cover's entries go when the cover does.
- `rate_limit.*` limits requests per caller. A request with a session or API key, or a download with its `media_token`, counts against that credential, and every credential of one user together gets four times `global_per_minute`; any other request counts against its client address (each IPv6 /64), read from `X-Forwarded-For`, `Forwarded` or `X-Real-IP` only when the peer is one of `rate_limit.trusted_proxies`. With authentication disabled every request counts against its address. Each caller gets `global_per_minute`, and a credential's API calls, besides covers and downloads, also `authenticated_per_minute`. Plugin routes authenticate their own way, so they are limited per client address. `/api/stream` is not request-limited; transcoding limits bound it instead. Login attempts get `login_per_minute` per client address. A request over a limit gets `429 Too Many Requests` with `Retry-After`.
- `transcoding.max_concurrent` caps encodes across HLS, streams and downloads, and `transcoding.max_concurrent_per_user` caps one user's stream and download encodes; `0` means no limit. Anonymous requests get a per-user allowance for each client address (each IPv6 /64), and together at most half of `transcoding.max_concurrent`, rounded down. A stream or download over a limit gets `429 Too Many Requests` with `Retry-After`; HLS waits up to ten seconds for a free slot first. Serving an original file or a cached transcode is never limited.

## Symbolic links in libraries

A library skips symbolic links by default: a sync counts them as skipped in the run's `discover` stage and logs how many there were and the first one, and a file replaced by a link counts as missing. Set `follow_links` on the library through the library API to follow them. The change reaches the catalog with the next sync, and a link's target must exist inside the container when Lyra runs in Docker.

With `follow_links` on:

- A sync follows links to folders, and to files whose real name is audio, a cue sheet or a `cover` image.
- A folder link is skipped when it leads into a library or a folder already followed. Links to files are not, so a file can be scanned twice. Broken links and loops fail a `discover` item on each sync, and what they held stays in the catalog.
- Entries keep the path of the link.
- Where libraries nest, a link goes by the setting of the deepest library holding it.

Whatever the setting, Lyra checks a library's file again each time it serves or registers it, and each time it reads metadata from it, since a link can change after a sync. The file must be a regular file inside a library the caller can see, or, in a library with `follow_links` on, outside every library and named as above. A link is refused when the deepest library holding its target is one the caller can't see.

Cover URLs are public, so they have no caller and every library counts as visible to them. In exchange, a cover outside every library must be named as a `cover` image, not just any media.
