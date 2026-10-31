local fixture = ...

local releases = require("@lyra/releases")

local by_id = {}
for _, release in releases.query({}).items do
	by_id[release.id] = release
end

local album = by_id[fixture.ids.album]
assert(album.release_title == "Album", "a summary keeps the release's fields")
assert(album.track_count == 2, `track count {album.track_count}`)
assert(album.total_duration == 4000, `total duration {album.total_duration}`)

local empty = by_id[fixture.ids.empty]
assert(empty.track_count == 0 and empty.total_duration == 0, "a release without tracks counts zero")
