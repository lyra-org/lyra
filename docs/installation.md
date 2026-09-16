# Installation

## 1. Start Lyra

Install Docker Compose and save this as `compose.yaml`. Replace `/path/to/music` with your music folder’s absolute path:

```yaml
services:
  lyra:
    image: registry.lyra.pub/lyra/lyra:latest
    restart: unless-stopped
    # user: "1000:1000" # Bind mounts: see permissions below.
    ports:
      - "4746:4746"
    volumes:
      - lyra-data:/data
      - lyra-plugins:/plugins
      - /path/to/music:/music:ro

volumes:
  lyra-data:
  lyra-plugins:
```

```sh
docker compose up -d
```

Fresh named volumes need no permission setup. Music is mounted read-only.

### Storage permissions

Lyra runs as `1000:1000`. To use bind mounts, create host folders and replace the volume names with their absolute paths.

- **Without user namespaces:** set `user:` to the host owner’s UID:GID.
- **Rootless Docker:** use `user: "0:0"` for folders owned by the daemon’s host user, or grant access to the mapped UID:GID.

To restore default storage ownership:

```sh
docker compose stop
docker compose run --rm --no-deps --user 0:0 --entrypoint chown lyra -R 1000:1000 /data /plugins
docker compose up -d
```

On bind mounts, this changes host ownership to the mapped IDs.

## 2. Add your music

Open [http://localhost:4746](http://localhost:4746) and create an account. The first account is the administrator.

[Install plugins](plugin-repositories.md), then add a library with `/music` as its path.

If Lyra runs on another computer, replace `localhost` with its address.

## Optional: use a custom web interface

Put the built frontend in `./static`. Set `LYRA_STATIC_DIR` and append the mount to the `lyra` service:

```yaml
    environment:
      LYRA_STATIC_DIR: /static
    volumes:
      - ./static:/static:ro
```

Apply with `docker compose up -d`.
