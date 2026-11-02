local fixture = ...
local call = require("./_call")
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

-- Music folders are the caller's libraries, under stable integer ids.
local music_folders = call.ok("getMusicFolders", key).musicFolders.musicFolder
assert(#music_folders == 1, `one visible library, got {#music_folders}`)
local folder_id = music_folders[1].id
assert(music_folders[1].name == "Music")
assert(type(folder_id) == "number" and folder_id == math.floor(folder_id) and folder_id > 0, "an integer id")
assert(folder_id == folders.id(fixture.libraries.music), "derived from the library id")
assert(folders.id(fixture.libraries.music) ~= folders.id(fixture.libraries.hidden), "per library")

-- An album carries its songs in disc and track order, with the caller's listens and ratings.
local album = call.ok("getAlbum", key, { id = ids.solo }).album
assert(album.id == ids.solo and album.name == "Solo")
assert(album.artist == "Amy" and album.artistId == ids.amy, `album artist {album.artist}`)
assert(album.songCount == 2 and album.duration == 380, `album totals {album.songCount} {album.duration}`)
assert(album.playCount == 2 and album.played == "2023-11-14T22:15:00Z", `album plays {album.played}`)
assert(album.userRating == 5 and album.starred == "2023-11-14T22:13:20Z", "album rating and star")
assert(album.genre == "Rock" and album.genres[1].name == "Rock")
assert(album.created ~= nil)
call.same(call.ids(album.song), { ids.s1, ids.s2 }, "album songs")

local s1 = album.song[1]
assert(s1.title == "S1" and s1.isDir == false and s1.type == "music" and s1.mediaType == "song")
assert(s1.parent == ids.solo and s1.albumId == ids.solo and s1.album == "Solo")
assert(s1.artist == "Amy" and s1.artistId == ids.amy and s1.displayArtist == "Amy")
assert(s1.albumArtists[1].id == ids.amy and s1.displayAlbumArtist == "Amy")
assert(s1.track == 1 and s1.discNumber == 1 and s1.year == 2001 and s1.duration == 180)
assert(s1.userRating == 4 and s1.playCount == 2, `song rating {s1.userRating} plays {s1.playCount}`)
assert(s1.played == "2023-11-14T22:15:00Z", `song played {s1.played}`)
assert(s1.suffix == "opus" and s1.contentType == "audio/ogg", `song format {s1.suffix} {s1.contentType}`)
assert(s1.size == 910, `song size {s1.size}`)
assert(s1.path == "Amy/Solo/song.opus", `a listener sees the library path, got {s1.path}`)
assert(s1.genre == "Rock")
assert(s1.displayComposer == "Bob", `composer {s1.displayComposer}`)
local roles = {}
for _, contributor in s1.contributors do
	roles[contributor.role] = contributor
end
assert(roles.composer and roles.composer.artist.id == ids.bob, "composer contributor")
assert(roles.performer and roles.performer.subRole == "piano", "performer contributor")
assert(album.song[2].playCount == 0 and album.song[2].played == nil, "an unplayed song")

assert(call.error_code("getAlbum", key, {}) == 10, "getAlbum needs an id")
assert(call.error_code("getAlbum", key, { id = ids.secret }) == 70, "a hidden album isn't found")
assert(call.error_code("getAlbum", key, { id = ids.s1 }) == 70, "a song isn't an album")
assert(call.error_code("getAlbum", key, { id = "not-an-id" }) == 70, "an unknown id isn't found")

-- A song reads the same.
local song = call.ok("getSong", key, { id = ids.s1 }).song
assert(song.id == ids.s1 and song.played == s1.played and song.path == s1.path)
assert(call.error_code("getSong", key, {}) == 10, "getSong needs an id")
assert(call.error_code("getSong", key, { id = ids.h1 }) == 70, "a hidden song isn't found")
assert(call.error_code("getSong", key, { id = ids.solo }) == 70, "an album isn't a song")

-- A caller who manages libraries sees the file's full path.
local curated = call.ok("getSong", fixture.api_keys.curator_key, { id = ids.s1 }).song
assert(string.sub(curated.path, 1, 1) == "/", `full path, got {curated.path}`)
assert(string.sub(curated.path, -#"/song.opus") == "/song.opus", curated.path)
assert(curated.playCount == 0 and curated.userRating == nil, "another user's listens and ratings")

-- XML carries the same album, with songs as repeated elements.
local xml = call.raw("getAlbum", { apiKey = key, id = ids.solo }) :: any
assert(string.find(xml.headers["content-type"], "text/xml", 1, true))
assert(string.find(xml.body, `<album `, 1, true), xml.body)
local _, song_elements = string.gsub(xml.body, "<song ", "")
assert(song_elements == 2, `two song elements, got {song_elements}`)
assert(string.find(xml.body, `<albumArtists id="{ids.amy}"`, 1, true), "nested lists are elements too")
