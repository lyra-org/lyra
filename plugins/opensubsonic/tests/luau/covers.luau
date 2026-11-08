local fixture, harness = ...
local call = require("./_call")(harness)
local covers = require("@lyra/covers")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function cover_art(params: { [string]: string }): any
	local query: { [string]: string } = { apiKey = key, f = "json" }
	for name, value in params do
		query[name] = value
	end
	return call.raw("getCoverArt", query)
end

-- `getCoverArt` fails in XML whatever the format asked for, as `stream` does.
local function cover_art_error(params: { [string]: string }): number
	local failed = cover_art(params)
	assert(failed.kind == "text", `getCoverArt answered {failed.kind}`)
	assert(string.find(failed.headers["content-type"], "^text/xml"), failed.headers["content-type"])
	local code = string.match(failed.body, '<error code="(%d+)"')
	assert(code, failed.body)
	return tonumber(code) :: number
end

local function expected_id(item_id: string): string
	local cover = covers.get({ item_id })[item_id]
	return `{cover.owner_id}.{string.sub(cover.hash, 1, 8)}`
end

-- An album's songs share its coverArt id; items without a cover leave it out.
local album = call.ok("getAlbum", key, { id = ids.album }).album
local album_art = expected_id(ids.album)
assert(album.coverArt == album_art, `album coverArt {album.coverArt}, expected {album_art}`)
assert(album.song[1].coverArt == album_art, `song coverArt {album.song[1].coverArt}`)
assert(call.ok("getSong", key, { id = ids.a1 }).song.coverArt == album_art, "getSong")
local bare = call.ok("getAlbum", key, { id = ids.bare }).album
assert(bare.coverArt == nil and bare.song[1].coverArt == nil, "no cover, no coverArt")

local listed = call.ok("getAlbumList2", key, { type = "alphabeticalByName" }).albumList2.album
local listed_art: { [string]: string } = {}
for _, item in listed do
	listed_art[item.id] = item.coverArt
end
assert(listed_art[ids.album] == album_art and listed_art[ids.bare] == nil, "getAlbumList2")

local xml = call.raw("getSong", { apiKey = key, id = ids.a1 }).body
assert(string.find(xml, `coverArt="{album_art}"`, 1, true), xml)

-- The image as stored.
local served = cover_art({ id = album_art })
local cover = covers.get({ ids.album })[ids.album]
assert(served.kind == "cover", `getCoverArt answered {served.kind}`)
assert(served.cover_id == cover.id, served.cover_id)
assert(served.options == nil, "no size serves the image as it is")
-- A file the host fails to serve answers with a Subsonic error in XML, not the host's.
assert(served.fallback.kind == "text" and string.find(served.fallback.body, '<error code="0"', 1, true))
assert(string.find(served.fallback.headers["content-type"], "^text/xml"), "a fallback in XML")
assert(cover_art({ id = album_art, size = "0" }).options == nil, "size 0 is the stored image")
assert(cover_art({ id = album_art, size = "-5" }).options == nil, "a negative size too")

-- `size` scales the image down to fit both sides, keeping its format.
local resized = cover_art({ id = album_art, size = "300" })
assert(resized.kind == "cover" and resized.cover_id == cover.id, `resized {resized.kind}`)
local options = resized.options
assert(options.max_width == 300 and options.max_height == 300, "fits both sides")
assert(options.format == nil, "the host keeps the stored format")
assert(resized.headers == nil, "the host types the resized image")
assert(resized.fallback ~= nil, "a resized image fails as the stored one does")
local huge = cover_art({ id = album_art, size = "99999999999" }).options
assert(huge.max_width == 99999999999, `the host caps a size past its bound, got {huge.max_width}`)

-- A bare item id, or one with a stale hash, names the item's current cover.
assert(cover_art({ id = ids.album }).cover_id == cover.id, "an album id")
assert(cover_art({ id = ids.a1 }).cover_id == cover.id, "a song id shows its album's")
assert(cover_art({ id = `{ids.album}.00000000` }).cover_id == cover.id, "a stale hash")

-- Playlists show the cover of a release in them, or none without one, in every response.
local function listed_playlist_art(): { [string]: string }
	local art: { [string]: string } = {}
	for _, playlist in call.ok("getPlaylists", key).playlists.playlist do
		art[playlist.id] = playlist.coverArt
	end
	return art
end
local playlist_art = listed_playlist_art()
assert(playlist_art[ids.mine] == album_art, `own playlist coverArt {playlist_art[ids.mine]}`)
assert(playlist_art[ids.shared] == album_art, "another user's public playlist")
assert(playlist_art[ids.plain] == nil, "a playlist without art leaves it out")
assert(playlist_art[ids.private] == nil, "another user's private playlist isn't listed")
assert(call.ok("getPlaylist", key, { id = ids.mine }).playlist.coverArt == album_art, "getPlaylist")
assert(call.ok("getPlaylist", key, { id = ids.plain }).playlist.coverArt == nil, "getPlaylist without art")
local created = call.ok("createPlaylist", key, { name = "New", songId = ids.a1 }).playlist
assert(created.coverArt == album_art, `createPlaylist coverArt {created.coverArt}`)
local emptied = call.ok("createPlaylist", key, { playlistId = created.id }).playlist
assert(emptied.coverArt == nil, "a playlist emptied of its songs has no art")
assert(cover_art({ id = ids.mine }).cover_id == cover.id, "a playlist id names its cover")

-- A playlist's own cover is named by the playlist's id.
local own_cover = covers.get({ ids.own })[ids.own]
assert(own_cover.owner_id == ids.own, "the playlist owns its cover")
local own_art = call.ok("getPlaylist", key, { id = ids.own }).playlist.coverArt
assert(own_art == expected_id(ids.own) and string.sub(own_art, 1, #ids.own + 1) == `{ids.own}.`, own_art)
local own = cover_art({ id = own_art })
assert(own.kind == "cover" and own.cover_id == own_cover.id, "the playlist's own cover")

-- Artists show their own cover; one without a cover leaves coverArt out.
local function artist_art(name: string): string?
	local found = call.ok("search3", key, { query = name, albumCount = "0", songCount = "0" }).searchResult3
	return found.artist[1].coverArt
end
assert(artist_art("Bob") == nil, "an artist without a cover")
local amy_cover = covers.get({ ids.amy })[ids.amy]
local amy_art = artist_art("Amy")
assert(amy_art == `{ids.amy}.{string.sub(amy_cover.hash, 1, 8)}`, `artist coverArt {amy_art}`)
local amy = cover_art({ id = amy_art :: string })
assert(amy.kind == "cover" and amy.cover_id == amy_cover.id, `artist {amy.kind}`)

-- The id's suffix is the start of the hash the host reports, which changes with the image.
assert(string.match(amy_art :: string, "%.(.*)$") == string.sub(amy_cover.hash, 1, 8), "an artist's suffix")
assert(string.match(own_art, "%.(.*)$") == string.sub(own_cover.hash, 1, 8), "a playlist's suffix")
assert(cover_art({ id = `{ids.amy}.00000000` }).cover_id == amy_cover.id, "a stale hash serves the current cover")
assert(cover_art({ id = ids.own }).cover_id == own_cover.id, "so does a bare playlist id")

-- Missing art, bad ids and what the caller can't see are not found.
assert(cover_art_error({ id = ids.bare }) == 70, "an album without a cover")
assert(cover_art_error({ id = ids.b1 }) == 70, "a song without one")
assert(cover_art_error({ id = "nonsense" }) == 70, "an unknown id")
assert(cover_art_error({ id = ".album" }) == 70, "no item id")
assert(cover_art_error({ id = `{ids.secret}.00000000` }) == 70, "an album in a hidden library")
assert(cover_art_error({ id = ids.h1 }) == 70, "a hidden song")
assert(cover_art_error({ id = ids.private }) == 70, "another user's private playlist")
local album_hash = string.match(album_art, "%.(.*)$") :: string
assert(cover_art_error({ id = `{ids.private}.{album_hash}` }) == 70, "with a cover's hash too")
assert(cover_art_error({}) == 10, "an id is required")
assert(cover_art_error({ id = album_art, size = "big" }) == 0, "a bad size")
assert(cover_art_error({ apiKey = "not-a-key", id = album_art }) == 44, "a key is required")

-- A cover whose file is gone is listed until it is served, then forgotten.
local gone_art = call.ok("getAlbum", key, { id = ids.gone }).album.coverArt
assert(gone_art ~= nil, "listings never look at the file")
assert(cover_art_error({ id = gone_art }) == 70, "the file is gone")
assert(call.ok("getAlbum", key, { id = ids.gone }).album.coverArt == nil, "the record is gone too")
