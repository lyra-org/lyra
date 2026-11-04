# Jellyfin Compatibility

Serves Jellyfin's API, so Jellyfin music clients can connect to Lyra. Point a client at your Lyra
server's address.

## Resume positions

A song's resume position (`PlaybackPositionTicks` in its user data) is your Lyra bookmark of it,
the same one OpenSubsonic clients and Lyra's own see. As in Jellyfin, playing a song never saves
a position: only a client updating the song's user data (`POST /UserItems/{itemId}/UserData`) sets
one, and a position of 0 clears it. Lyra keeps positions to the millisecond, so a position under
one millisecond clears it too, and a position past the song's end is kept as its end.

Only songs hold a position. An update giving one for an album, artist, playlist or genre still
applies its other fields, and the item's position stays 0, as for the items Jellyfin keeps none
for.

A user data update can also favorite or unfavorite the item (`IsFavorite`). Your listens alone
decide `Played`, `PlayCount` and `LastPlayedDate`, so an update's values for them are ignored, as
are `Rating` and `Likes`. Unlike Jellyfin, a negative position is refused.

The resume lists (`/UserItems/Resume`) are always empty, though Jellyfin lists every song with a
position there.
