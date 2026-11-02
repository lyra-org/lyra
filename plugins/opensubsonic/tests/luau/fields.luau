local fixture = ...
local call = require("./_call")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function empty(value: any, label: string)
	assert(type(value) == "table" and next(value) == nil, `{label} is empty, got {tostring(value)}`)
end

-- OpenSubsonic fields come back empty when Lyra has nothing for them, and `created` always.
local album = call.ok("getAlbum", key, { id = ids.bare }).album
empty(album.genres, "album genres")
empty(album.artists, "album artists")
empty(album.releaseTypes, "album releaseTypes")
empty(album.releaseDate, "album releaseDate")
assert(album.displayArtist == "" and album.sortName == "", "album strings")
assert(album.created == "1970-01-01T00:05:00Z", `the earliest track's time, got {album.created}`)
assert(album.userRating == nil and album.played == nil, "left out when absent")

local song = album.song[1]
empty(song.genres, "song genres")
empty(song.artists, "song artists")
empty(song.albumArtists, "song albumArtists")
empty(song.contributors, "song contributors")
empty(song.replayGain, "song replayGain")
assert(song.displayArtist == "" and song.displayAlbumArtist == "" and song.displayComposer == "")
assert(song.sortName == "", "song sortName")
assert(song.bitDepth == 0 and song.samplingRate == 0 and song.channelCount == 0, "unknown audio")
assert(song.userRating == nil and song.played == nil, "left out when absent")

local lone = call.ok("getAlbum", key, { id = ids.empty }).album
assert(lone.created == "1970-01-01T00:00:00Z", `a fixed time without any, got {lone.created}`)
assert(lone.artists[1].id == ids.zed and lone.artists[1].sortName == "", "artist refs carry sortName")

local artists = call.ok("search3", key, { query = "Zed", albumCount = "0", songCount = "0" }).searchResult3.artist
assert(artists[1].id == ids.zed and artists[1].sortName == "", "artist sortName")

-- XML keeps empty objects as elements and leaves empty lists out.
local xml = call.raw("getSong", { apiKey = key, id = ids.b1 }).body
assert(string.find(xml, "<replayGain/>", 1, true), xml)
assert(string.find(xml, 'displayArtist=""', 1, true), xml)
assert(not string.find(xml, "<genres", 1, true), xml)
local album_xml = call.raw("getAlbum", { apiKey = key, id = ids.bare }).body
assert(string.find(album_xml, "<releaseDate/>", 1, true), album_xml)
