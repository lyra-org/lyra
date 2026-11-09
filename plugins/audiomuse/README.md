# AudioMuse AI

Builds mixes from how your music sounds, using an
[AudioMuse AI](https://github.com/NeptuneHub/AudioMuse-AI) server.

## Setup

AudioMuse reads your library from Lyra through the OpenSubsonic or Jellyfin plugin, so install the
one you want it to use and create a Lyra API key from your account. Jellyfin takes more setup,
because AudioMuse also asks for your user ID.

### OpenSubsonic

In AudioMuse's setup, choose Navidrome and select API key authentication:

- **URL:** your Lyra server's address, such as `https://lyra.example.org`
- **API key:** your Lyra API key

### Jellyfin

In AudioMuse's setup, choose Jellyfin:

- **URL:** your Lyra server's address followed by `/jellyfin`, such as
  `https://lyra.example.org/jellyfin`
- **API token:** your Lyra API key
- **User ID:** the `Id` that this command returns

```sh
curl -H 'Authorization: MediaBrowser Token="YOUR_API_KEY"' \
  https://lyra.example.org/jellyfin/Users/Me
```

The user ID must belong to the account that owns the API key.

### Lyra

Run AudioMuse's analysis, then set these in the plugin's settings in Lyra:

- **Base URL:** your AudioMuse server's address
- **Token:** your AudioMuse API token, if it requires one
- **Media server:** the API you chose above
- **AudioMuse server:** the name or ID Lyra is registered under in AudioMuse, if it isn't the
  default server

The two APIs give tracks different IDs, so changing **Media server** later means analyzing your
library again.
