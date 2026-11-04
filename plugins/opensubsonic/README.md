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

## Supported methods

Lyra serves these [methods](https://opensubsonic.netlify.app/docs/endpoints/), grouped as the
specification groups them. Others fail with error 0.

- **Bookmarks:** [`createBookmark`](#bookmarks), [`deleteBookmark`](#bookmarks),
  [`getBookmarks`](#bookmarks), [`getPlayQueue`](#play-queues),
  [`getPlayQueueByIndex`](#play-queues), [`savePlayQueue`](#play-queues),
  [`savePlayQueueByIndex`](#play-queues)
- **Browsing:** [`findSonicPath`](#sonic-similarity), `getAlbum`, `getAlbumInfo`, `getAlbumInfo2`,
  `getArtist`, `getArtistInfo`, `getArtistInfo2`, `getArtists`, `getGenres`,
  [`getIndexes`](#browsing-by-folder), [`getMusicDirectory`](#browsing-by-folder),
  `getMusicFolders`, `getSimilarSongs`, `getSimilarSongs2`, `getSong`,
  [`getSonicSimilarTracks`](#sonic-similarity), [`getTopSongs`](#top-songs)
- **Lists:** `getAlbumList`, `getAlbumList2`, [`getNowPlaying`](#now-playing), `getRandomSongs`,
  `getSongsByGenre`, `getStarred`, `getStarred2`
- **Media annotation:** [`reportPlayback`](#now-playing), `scrobble`, `setRating`, `star`, `unstar`
- **Media library scanning:** `getScanStatus`, `startScan`
- **Media retrieval:** [`download`](#downloading), `getCoverArt`, `getLyrics`, `getLyricsBySongId`,
  `stream`
- **Playlists:** `createPlaylist`, `deletePlaylist`, `getPlaylist`, `getPlaylists`, `updatePlaylist`
- **Searching:** `search2`, `search3`
- **System:** `getLicense`, `getOpenSubsonicExtensions`, `ping`, `tokenInfo`
- **Transcoding:** [`getTranscodeDecision`](#transcoding), [`getTranscodeStream`](#transcoding)
- **User management:** `getUser`, `getUsers`

Extensions: `apiKeyAuthentication`, `formPost`, `indexBasedQueue`, `playbackReport`, `songLyrics`,
`topSongsByArtistId`, `transcodeOffset`, [`transcoding`](#transcoding), and
[`sonicSimilarity`](#sonic-similarity) while a mixer is set.

Not served: sharing, podcasts, internet radio, jukebox, chat, video, avatars, HLS, user management
beyond reading users, and the deprecated `search`.

## Play queues

Each client saves its own play queue, named by the client name it sends, and Lyra keeps it as a
playback of your account. `getPlayQueue` returns your most recently changed queue from any client,
Lyra's own included, so you can pick up on one device where you left off on another. A queue is cut
to 1,000 songs around its current song. Clients supporting `indexBasedQueue` save and read the
current song by its place in the queue instead, which tells repeats of a song apart.

Saving an empty queue deletes only that client's queue, after which `getPlayQueue` returns the most
recent queue of another client, if there is one. Other servers keep one queue per user, which an
empty save clears.

## Now playing

Clients supporting `playbackReport` report what they play as they play it, and Lyra counts a song
as played once enough of it plays, as it does for its own clients. Other clients' "now playing"
scrobbles show the song playing without counting it; their submitted scrobble counts it.
Lyra tells players apart by client name and the app's User-Agent, so two devices using the same
app show as one player only when both send the same User-Agent. Updating an app can change its
User-Agent, which makes it a new player.

`getNowPlaying` lists only your own playing songs, from every client, Lyra's own included. Other
users' are never shown.

## Bookmarks

A bookmark saves your place in a song, with an optional comment of up to 1,000 characters. Only
songs take bookmarks, one per song: saving another replaces it. A position past the song's end is
kept as its end. Bookmarks are separate from play queues and listens, and nothing saves or clears
them for you.

## Browsing by folder

Clients that browse by folder (`getIndexes`, `getMusicDirectory`) see artists, their albums and
the albums' songs, not the folders on disk. The top folders are the artists credited on albums,
each holding its albums, oldest first; an album holds its songs in disc and track order, and sits
under its first artist. Music folders choose which libraries to list, and aren't folders to open.

## Top songs

An artist's top songs (`getTopSongs`) are your own most played songs of that artist, from your
listens alone, so an artist you never played has none. A name finds every artist of that name;
clients supporting `topSongsByArtistId` can name one artist by its ID instead.

## Downloading

Clients download songs as their original files, which takes the download permission on your Lyra
account. Without it, a download fails with error 50, while streaming still works.

## Transcoding

Clients supporting `transcoding` describe what they can play, and Lyra answers whether it sends a
song as it is or transcodes it, and how. A song plays as it is through the first direct play
profile that takes it. Otherwise Lyra transcodes it for the first transcoding profile it can meet,
lowered as far as the client's limits ask, but never upsampled, upmixed, or made lossless from a
lossy file. A lossy file keeps its own bitrate unless a limit lowers it. Lyra streams over HTTP
only, so profiles asking for HLS are skipped.

A limit marked as not required lowers a transcode where it can, but never rules a profile out.
Lyra settles a transcode's channels, then its sample rate, then its bit depth or bitrate, each
once and with the limits on that property alone, so it can miss a combination such as more
channels to reach a required bitrate.
A required one that Lyra can't check, such as an AAC profile or a limit it doesn't know, counts
as unmet. Containers, codecs, and protocols Lyra doesn't know match nothing.

The decision's `transcodeParams` stay valid while the song's file is unchanged; once its format
or audio properties change, `getTranscodeStream` answers 410 and the client asks for a new
decision.

## Sonic similarity

Clients that support the `sonicSimilarity` extension can find songs that sound like a song
(`getSonicSimilarTracks`) and paths of songs from one song to another (`findSonicPath`). Lyra
serves them through a mixer, such as AudioMuse AI's: set the plugin's **Mixer** setting to the
mixer's ID, such as `audiomuse`. The extension is offered only while that mixer is enabled and finds
both similar songs and paths. Leaving the setting empty turns it off.

A song the mixer gave no similarity shows `-1`, as AudioMuse AI's paths do.
