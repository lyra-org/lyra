# Lyra

Lyra is a music server with a Luau plugin system and an emphasis on metadata correctness.

> [!IMPORTANT]
>
> This project is currently in pre-alpha. It is meant for developers and early testers who are willing to endure crashes and data resets.

## Installation

The recommended installation method is to use Cargo:
```bash
cargo +nightly install --locked --git https://github.com/lyra-org/lyra lyra-server
```

You may then run the installed binary to run:
```bash
lyra serve
```

## Configuration

The runtime configuration is loaded from `config.json` and deserialized by
`lyra-server/src/config.rs`.

### Schema

```ts
type Config = {
  port?: number; // u16, default 4746
  published_url?: string | null; // http/https URL only
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
    path?: string; // default "lyra.db"
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
    signed_url_ttl_seconds?: number | null; // unset or <= 0 uses 90 seconds
    cleanup_startup_purge?: boolean | null; // unset uses true
    max_concurrent_transcodes?: number | null; // unset or 0 means unlimited
  };
};
```

## License

This project is licensed under the [Lyra Public License, Version 1.0](LICENSE). While this license is custom, it is based on the [MPL-2.0](https://opensource.org/license/MPL-2.0) and includes only an additional provision regarding Remote Network Interaction, inspired by the [AGPL-3.0](https://opensource.org/license/agpl-3-0). You are free to use this project as you see fit, so long as you comply with the license's terms.
