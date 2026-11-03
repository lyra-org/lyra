local fixture, harness = ...
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function starred(params: { [string]: string }?): any
	return call.ok("getStarred2", key, params).starred2
end

local function song(id: string): any
	return call.ok("getSong", key, { id = id }).song
end

-- Starring takes songs, albums and artists alike, each parameter repeatable, and lists the most
-- recently starred first.
call.ok("star", key, { id = ids.s1, albumId = ids.solo, artistId = ids.amy })
local all = starred()
call.same(call.ids(all.artist), { ids.amy }, "starred artists")
call.same(call.ids(all.album), { ids.solo }, "starred albums")
call.same(call.ids(all.song), { ids.s1, ids.f1 }, "starred songs")
assert(all.song[2].starred == "2023-11-14T22:13:20Z", all.song[2].starred)
assert(type(all.album[1].starred) == "string" and type(all.artist[1].starred) == "string", "star times")

local extra = tostring(folders.id(fixture.libraries.extra))
local in_extra = starred({ musicFolderId = extra })
call.same(call.ids(in_extra.song), { ids.f1 }, "starred songs in a folder")
assert(in_extra.album == nil and in_extra.artist == nil, "nothing else starred in the folder")
local hidden_folder = tostring(folders.id(fixture.libraries.hidden))
assert(call.error_code("getStarred2", key, { musicFolderId = hidden_folder }) == 70, "a hidden folder")

-- Unseen items are skipped.
call.ok("star", key, { id = { ids.s2, ids.h1 } })
call.same(call.ids(starred().song), { ids.s2, ids.s1, ids.f1 }, "the visible song starred")
assert(call.error_code("star", key, {}) == 10, "starring needs an id")
call.ok("star", key, { id = ids.s1 })

call.ok("unstar", key, { id = { ids.s1, ids.s2 }, albumId = ids.solo, artistId = ids.amy })
call.ok("unstar", key, { id = ids.s1 })
local after = starred()
call.same(call.ids(after.song), { ids.f1 }, "unstarred songs")
assert(after.album == nil and after.artist == nil, "unstarred album and artist")
call.ok("unstar", key, { id = ids.h1 })
assert(call.error_code("unstar", key, {}) == 10, "unstarring needs an id")

-- Stars are the caller's own.
local neighbours = call.ok("getStarred2", fixture.api_keys.neighbour_key).starred2
assert(type(neighbours) == "table" and next(neighbours) == nil, "the neighbour starred nothing")

-- Ratings are 1-5, and 0 clears one.
call.ok("setRating", key, { id = ids.s1, rating = "3" })
assert(song(ids.s1).userRating == 3, "a song rating")
call.ok("setRating", key, { id = ids.solo, rating = "5" })
assert(call.ok("getAlbum", key, { id = ids.solo }).album.userRating == 5, "an album rating")
call.ok("setRating", key, { id = ids.s1, rating = "0" })
assert(song(ids.s1).userRating == nil, "a cleared rating")
call.ok("setRating", key, { id = ids.s1, rating = "0" })
assert(call.error_code("setRating", key, { id = ids.s1, rating = "6" }) == 0, "a rating past 5")
assert(call.error_code("setRating", key, { id = ids.s1, rating = "-1" }) == 0, "a negative rating")
assert(call.error_code("setRating", key, { id = ids.s1, rating = "good" }) == 0, "a rating is an integer")
assert(call.error_code("setRating", key, { id = ids.s1 }) == 10, "a rating is required")
assert(call.error_code("setRating", key, { rating = "3" }) == 10, "an id is required")
assert(call.error_code("setRating", key, { id = ids.h1, rating = "3" }) == 70, "rating a hidden song")

-- A scrobble counts the song as played from the time given, once however often it is sent.
assert(song(ids.s1).playCount == 0, "unplayed")
call.ok("scrobble", key, { id = ids.s1, time = "1700000000000" })
assert(song(ids.s1).playCount == 1, "a scrobble")
assert(song(ids.s1).played == "2023-11-14T22:13:20Z", song(ids.s1).played)
call.ok("scrobble", key, { id = ids.s1, time = "1700000000000", submission = "true" })
assert(song(ids.s1).playCount == 1, "a repeated scrobble")

-- Offline plays come in batches.
call.ok("scrobble", key, { id = { ids.s1, ids.s2 }, time = { "1700000500000", "1700000700000" } })
assert(song(ids.s1).playCount == 2 and song(ids.s2).playCount == 1, "a batch")

-- The same song at the same time twice in one request is two plays, still counted once each when
-- the request is retried.
local repeated = { id = { ids.s2, ids.s2 }, time = { "1700001000000", "1700001000000" } }
call.ok("scrobble", key, repeated)
assert(song(ids.s2).playCount == 3, "a repeated play in one batch")
call.ok("scrobble", key, repeated)
assert(song(ids.s2).playCount == 3, "a retried batch with a repeated play")

-- Without a time, the song ends now.
call.ok("scrobble", key, { id = ids.s2 })
assert(song(ids.s2).playCount == 4, "a scrobble without a time")

-- A time at the end of the song, as when a client sends the time it scrobbled, still counts.
call.ok("scrobble", key, { id = ids.s2, time = tostring(os.time() * 1000) })
assert(song(ids.s2).playCount == 5, "a scrobble timed at its end")

-- A song of unknown duration counts too.
call.ok("scrobble", key, { id = ids.s3, time = "1700000000000" })
call.ok("scrobble", key, { id = ids.s3 })
assert(song(ids.s3).playCount == 2, "a song of unknown duration")

-- "Now playing" notifications count nothing.
call.ok("scrobble", key, { id = ids.s2, submission = "false" })
assert(song(ids.s2).playCount == 5, "now playing")

-- Hidden songs count nothing, and don't fail the visible ones.
call.ok("scrobble", key, { id = { ids.h1, ids.f1 }, time = { "1700000000000", "1700000000000" } })
assert(song(ids.f1).playCount == 1, "a batch with a hidden song")

assert(call.error_code("scrobble", key, {}) == 10, "a scrobble needs an id")
assert(
	call.error_code("scrobble", key, { id = { ids.s1, ids.s2 }, time = "1700000000000" }) == 0,
	"a time for each song"
)
assert(call.error_code("scrobble", key, { id = ids.s1, time = "-5" }) == 0, "a negative time")
assert(call.error_code("scrobble", key, { id = ids.s1, time = "1e100" }) == 0, "a time past the host's range")

-- An out-of-range time fails the request before any of it is counted, however many chunks it spans.
local many_ids, many_times = {}, {}
for index = 1, 600 do
	table.insert(many_ids, ids.s1)
	table.insert(many_times, tostring(1700002000000 + index * 1000))
end
many_times[600] = "1e100"
assert(call.error_code("scrobble", key, { id = many_ids, time = many_times }) == 0, "a long batch with a bad time")
assert(song(ids.s1).playCount == 2, "nothing of the long batch counted")
assert(call.error_code("scrobble", key, { id = ids.s1, submission = "maybe" }) == 0, "submission is a boolean")
