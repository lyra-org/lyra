# Lyra

Lyra is a music server with a Luau plugin system and an emphasis on metadata correctness.

> [!IMPORTANT]
>
> This project is currently in pre-alpha. It is meant for developers and early testers who are willing to endure crashes and data resets.

## Installation

The recommended installation method is Docker Compose. Create a directory for Lyra, put any web UI/static assets in `./static`, and start the container with a compose file like this:

```yaml
services:
  lyra:
    image: registry.lyra.pub/lyra/lyra:latest
    restart: unless-stopped
    ports:
      - "4746:4746"
    # These match the image defaults. Uncomment and adjust them only if you
    # mount server state, plugins, or static assets somewhere else.
    # environment:
    #   LYRA_DATA_DIR: /lyra/data
    #   LYRA_PLUGINS_DIR: /lyra/plugins
    #   LYRA_STATIC_DIR: /lyra/static
    volumes:
      - ./static:/lyra/static:ro
      - lyra-data:/lyra/data
      - /path/to/music:/music:ro

volumes:
  lyra-data:
```

Then start Lyra:

```bash
docker compose up -d
```

No `config.json` is required: the container stores its database and covers under `/lyra/data` (the `lyra-data` volume) and listens on port 4746. To configure a library or anything else from a file, mount one at `/lyra/config.json` using container paths:

```yaml
    volumes:
      - ./config.json:/lyra/config.json:ro
```

```json
{
  "library": {
    "path": "/music"
  }
}
```

`LYRA_DATA_DIR` is the root for everything the server owns (database, covers). The image defaults it to `/lyra/data`; change both the environment value and volume target if you want to mount the state somewhere else.

`LYRA_STATIC_DIR` controls the directory used for static assets inside the container. The image defaults it to `/lyra/static`; change both the environment value and volume target if you want to mount the assets somewhere else.

Cargo installation is still useful for local development:

```bash
cargo install --locked --git https://git.lyra.pub/lyra/lyra lyra-server
```

You may then run the installed binary to start the server with `serve`:
```bash
lyra serve
```

It is highly recommended that you also grab the plugins in `plugins`, especially the MusicBrainz plugin, and drop them into a `plugins` directory where you run the binary from.

## Configuration

Lyra starts without any configuration: the database is stored under the data directory (`./data` next to where you run the binary, `/lyra/data` in Docker) and the server listens on port 4746.

An optional `config.json` refines that. It is looked up in the working directory, next to the binary, and in the source tree; set `LYRA_CONFIG_PATH` to load a specific file (the server fails to start if that file is missing). `LYRA_PORT` takes precedence over `port` from the file. Unknown keys in the file are rejected, so a misspelled or removed key fails startup instead of being ignored.

### Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `LYRA_CONFIG_PATH` | searched | Explicit path to `config.json`; must exist when set |
| `LYRA_DATA_DIR` | `./data` | Root for server-owned state; created when serving |
| `LYRA_DB_DIR` | data dir | Directory for relative `db.path` values; created when serving |
| `LYRA_PORT` | `4746` | Listening port; overrides `port` from the file |
| `LYRA_PLUGINS_DIR` | `./plugins` | Directory plugins are loaded from |
| `LYRA_STATIC_DIR` | searched | Directory for static web assets |

Set `"kind"` in `"db"` to `"memory"` for a throwaway database that does not touch the disk.

### Schema

```ts
type Config = {
  port?: number; // u16, default 4746
  published_url?: string | null; // public http/https origin only
  cors?: {
    allowed_origins?: string[]; // default []
  };
  library?: {
    path?: string | null;
    language?: string | null; // ISO 639-1, ISO 639-3, or language name
    country?: string | null; // country code or name
  } | null;
  covers_path?: string | null; // default "<data dir>/covers"

  db?: {
    kind?: "memory" | "file" | "mmap"; // default "mmap"
    path?: string; // default "lyra.db"; relative paths resolve under LYRA_DB_DIR, else the data dir
  };

  auth?: {
    enabled?: boolean; // default true
    allow_default_login_when_disabled?: boolean; // default true
    session_ttl_seconds?: number; // u64, default 2592000
  };

  sync?: {
    interval_secs?: number; // u64, default 0
  };

  hls?: {
    temp_disk_budget_bytes?: number | null; // unset or <= 0 means no budget
    cleanup_startup_purge?: boolean | null; // unset uses true
    max_concurrent_transcodes?: number | null; // unset or 0 means unlimited
  };
};
```

## License

This project is licensed under the [Lyra Public License, Version 1.0](LICENSE) (LPL-1.0). While this license is custom, it is based on the [MPL-2.0](https://opensource.org/license/MPL-2.0).

The main differences between the two are that the `LPL-1.0` includes an additional provision regarding Remote Network Interaction (inspired by the [AGPL-3.0](https://opensource.org/license/agpl-3-0)) and limits your secondary license options to only the `AGPL-3.0-or-later`.

You are free to use this project as you see fit, so long as you comply with the license's terms.
