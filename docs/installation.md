# Installation

## 1. Start Lyra

You’ll need Docker Compose installed. Create a folder for Lyra and save this as `compose.yaml`, replacing `/path/to/music` with your music folder’s full path:

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

From that folder, run:

```sh
docker compose up -d
```

Your server is now running at `http://localhost:4746`. Its data is saved in the `lyra-data` Docker volume, and installed plugins persist in `lyra-plugins`.

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

Install any plugins you want, then add a library. Use `/music` as the path: that is where Docker makes your music folder available to Lyra.

If Lyra runs on another computer, replace `localhost` with its address.

## Optional: set up through the API

### Create an account

The first account is the administrator. Replace `replace-with-your-password` in both commands with your own password of at least eight ASCII characters.

```sh
curl --fail-with-body http://localhost:4746/api/users \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"replace-with-your-password"}'
```

### Sign in

Sign in with the same credentials:

```sh
curl --fail-with-body http://localhost:4746/api/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"replace-with-your-password"}'
```

The response contains a `token`. Use that value in place of `YOUR_SESSION_TOKEN` below.

### Choose plugins

Docker images start without plugins. [Install any plugins you want](plugin-repositories.md)
before adding your library so metadata providers can participate in its first scan.

### Add your music

Use `/music` below: that is where Docker makes your music folder available to Lyra.

```sh
curl --fail-with-body http://localhost:4746/api/libraries \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_SESSION_TOKEN' \
  -d '{"name":"Music","directory":"/music"}'
```

Lyra now scans your music. You can keep the default settings, or [change them](configuration.md) later.

## Optional: use a custom web interface

To use your own web interface instead of the bundled one, put its built files (including `index.html`) in a `static` folder beside `compose.yaml`. Add this block to the `lyra` service:

```yaml
    environment:
      LYRA_STATIC_DIR: /static
```

Add this line under `volumes`, leaving the existing data, plugins, and music mounts in place:

```yaml
      - ./static:/static:ro
```

Run `docker compose up -d` again, then open `http://localhost:4746` in your browser.
