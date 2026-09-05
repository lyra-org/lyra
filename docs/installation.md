# Installation

## 1. Start Lyra

You’ll need Docker Compose installed. Create a folder for Lyra and save this as `compose.yaml`, replacing `/path/to/music` with your music folder’s full path:

```yaml
services:
  lyra:
    image: registry.lyra.pub/lyra/lyra:latest
    restart: unless-stopped
    ports:
      - "4746:4746"
    volumes:
      - lyra-data:/lyra/data
      - /path/to/music:/music:ro

volumes:
  lyra-data:
```

From that folder, run:

```sh
docker compose up -d
```

Your server is now running at `http://localhost:4746`. Its data is saved in the `lyra-data` Docker volume.

## 2. Add your music

Lyra does not include a web interface yet. Use the commands below to set up your library. If Lyra runs on another computer, replace `localhost` with its address.

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

### Add your music

Use `/music` below: that is where Docker makes your music folder available to Lyra.

```sh
curl --fail-with-body http://localhost:4746/api/libraries \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_SESSION_TOKEN' \
  -d '{"name":"Music","directory":"/music"}'
```

Lyra now scans your music. You can keep the default settings, or [change them](configuration.md) later.

## Optional: serve a web interface

If you have a web interface built for Lyra, put its files (including `index.html`) in a `static` folder beside `compose.yaml`. Add this line under `volumes` in the `lyra` service:

```yaml
      - ./static:/lyra/static:ro
```

Run `docker compose up -d` again, then open `http://localhost:4746` in your browser.
