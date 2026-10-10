# OpenSubsonic Compatibility

Serves the [OpenSubsonic API](https://opensubsonic.netlify.app/) at `/rest`, so Subsonic and
OpenSubsonic clients can connect to Lyra. Point a client at your Lyra server's address.

## Signing in

Clients sign in with a Lyra API key, never your account password. Create a key for each client
from your Lyra account's API keys, then either:

- enter the key in the client's API key field, if it has one, or
- enter your username, and the key as the password, with the client set to send the password
  in plain text. Clients call this "legacy authentication" or "plain text password".

Lyra can't check Subsonic tokens (`t`/`s`), so a client using token authentication fails with
error 41 until it is switched to one of the options above. Revoking a key signs out the client
that uses it.
