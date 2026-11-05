local fixture, harness = ...
local call = require("./_call")(harness)

local ids = fixture.ids
local key = fixture.api_keys.listener_key

-- Clients can't show songs whose files are missing, so lists and counts leave them out.
local found = call.ok("search3", key, { query = "" }).searchResult3
call.same(call.ids(found.song), { ids.playing }, "searched songs")
call.same(call.ids(found.album), { ids.mixed }, "searched albums")
assert(found.artist[1].albumCount == 1, `album count {found.artist[1].albumCount}`)

local album = call.ok("getAlbum", key, { id = ids.mixed }).album
call.same(call.ids(album.song), { ids.playing }, "an album's songs")
assert(album.songCount == 1, `song count {album.songCount}`)
assert(call.error_code("getAlbum", key, { id = ids.lost }) == 70, "an album with nothing to play")

local random = call.ok("getRandomSongs", key, { size = "10" }).randomSongs
call.same(call.ids(random.song), { ids.playing }, "random songs")
local starred = call.ok("getStarred2", key).starred2
assert(starred.song == nil, "a starred missing song")

-- Asked for directly, a missing song answers that its file is missing.
local song = call.call("getSong", key, { id = ids.missing })
assert(song.status == "failed" and song.error.code == 70, "getSong of a missing song")
assert(song.error.message == "The song's file is missing", song.error.message)
local streamed = call.raw("stream", { apiKey = key, id = ids.missing })
local body = call.body(streamed)
assert(string.find(body, 'code="70"', 1, true), body)

-- A playlist shows only what can play, and its indexes count what it shows.
local playlist_id = fixture.ids.mine
local listed = call.ok("getPlaylist", key, { id = playlist_id }).playlist
call.same(call.ids(listed.entry), { ids.playing }, "a playlist's songs")
assert(listed.songCount == 1, `playlist song count {listed.songCount}`)
call.ok("updatePlaylist", key, { playlistId = playlist_id, songIndexToRemove = "0" })
local updated = call.ok("getPlaylist", key, { id = playlist_id }).playlist
assert(updated.entry == nil, "the shown song was removed")

-- A song whose file went missing while it played doesn't show as playing.
call.ok("reportPlayback", key, {
	mediaId = ids.missing,
	mediaType = "song",
	positionMs = "0",
	state = "playing",
	c = "Phone",
})
local playing = call.ok("getNowPlaying", key).nowPlaying
assert(playing.entry == nil, "a missing song shows as playing")
