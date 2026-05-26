# Lyra

Lyra is a music server with a Luau plugin system and an emphasis on metadata correctness.

> [!IMPORTANT]
>
> This project is currently in pre-alpha. It is meant for developers and early testers who are willing to endure crashes and data resets.

## Installation

The recommended installation method is Docker Compose. Create a directory for Lyra, add a `config.json`, put any web UI/static assets in `./static`, and start the container with a compose file like this:

```yaml
services:
  lyra:
    image: registry.lyra.pub/lyra/lyra:latest
    restart: unless-stopped
    ports:
      - "4746:4746"
    # These match the image defaults. Uncomment and adjust them only if you
    # mount config, the database, plugins, or static assets somewhere else.
    # environment:
    #   LYRA_CONFIG_PATH: /lyra/config.json
    #   LYRA_DB_DIR: /lyra/data
    #   LYRA_PLUGINS_DIR: /lyra/plugins
    #   LYRA_STATIC_DIR: /lyra/static
    volumes:
      - ./config.json:/lyra/config.json:ro
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

Use container paths in `config.json`. A minimal persistent setup looks like this:

```json
{
  "library": {
    "path": "/music"
  },
  "covers_path": "/lyra/data/covers",
  "db": {
    "kind": "mmap"
  }
}
```

`LYRA_DB_DIR` controls the directory used for relative `db.path` values inside the container. The image defaults it to `/lyra/data`, so omitting `db.path` stores the persistent database at `/lyra/data/lyra.db`; change both the environment value and volume target if you want to mount the database somewhere else. Absolute `db.path` values are used as written.

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

The runtime configuration is loaded from `config.json`. In Docker, the default path is `/lyra/config.json`; for local Cargo installs, you can drop it in the same directory as `plugins`.

We recommend that you set the `"kind"` in `"db"` to `"mmap"` or `"file"` for persistence.

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
  covers_path?: string | null;

  db?: {
    kind?: "memory" | "file" | "mmap"; // default "memory"
    path?: string; // default "lyra.db"; relative paths use LYRA_DB_DIR when set
  };

  auth?: {
    enabled?: boolean; // default true
    allow_default_login_when_disabled?: boolean; // default true
    default_username?: string; // default "default"
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
