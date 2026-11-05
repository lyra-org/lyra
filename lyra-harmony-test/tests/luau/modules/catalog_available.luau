local fixture = ...
local artists = require("@lyra/artists")
local entries = require("@lyra/entries")
local genres = require("@lyra/genres")
local playback_sources = require("@lyra/playback_sources")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local ids = fixture.ids

local function names(items: { any }, field: string): string
	local found = {}
	for _, item in items do
		table.insert(found, item[field])
	end
	table.sort(found)
	return table.concat(found, ",")
end

local function same(actual: any, expected: any, label: string)
	assert(actual == expected, `{label}: expected {expected}, got {actual}`)
end

-- A track is unavailable when none of its files is there; reads by id still return it.
local found = tracks.get({ ids.playing, ids.missing })
same(found[ids.playing].available, true, "a playing track")
same(found[ids.missing].available, false, "a missing track")

same(names(tracks.query({}).items, "track_title"), "Lost One,Missing,Playing", "every track")
same(names(tracks.query({ available = true }).items, "track_title"), "Playing", "available tracks")
same(names(tracks.query({ available = false }).items, "track_title"), "Lost One,Missing", "unavailable tracks")
same(tracks.query({ available = true, release_ids = { ids.mixed } }).total, 1, "a release's playable tracks")
same(tracks.query({ available = true, ids = { ids.missing, ids.playing } }).total, 1, "playable tracks by id")
same(releases.query({ available = true, ids = { ids.lost } }).total, 0, "an unavailable release by id")

-- A release is available while any of its tracks is; the filter narrows what it counts.
local release_list = releases.get({ ids.mixed, ids.lost })
same(release_list[ids.mixed].available, true, "a partly missing release")
same(release_list[ids.lost].available, false, "a release with every file missing")
same(names(releases.query({ available = true }).items, "release_title"), "Mixed", "available releases")
same(
	names(releases.query({ available = false }).items, "release_title"),
	"Lost,Mixed",
	"releases holding missing tracks"
)
local function track_count(query: releases.ReleaseQuery): number
	query.ids = { ids.mixed }
	return releases.query(query).items[1].track_count
end
same(track_count({}), 2, "every track counts")
same(track_count({ available = true }), 1, "available counts its playable tracks")
same(track_count({ available = false }), 1, "unavailable counts its missing tracks")

-- An artist or genre is unavailable when everything it holds is.
local artist_list = artists.get({ ids.here, ids.gone })
same(artist_list[ids.here].available, true, "an artist with something to play")
same(artist_list[ids.gone].available, false, "an artist with nothing to play")
same(names(artists.query({ available = true }).items, "artist_name"), "Here", "available artists")
local genre_list = genres.get({ ids.rock, ids.jazz })
same(genre_list[ids.rock].available, true, "a genre with something to play")
same(genre_list[ids.jazz].available, false, "a genre with nothing to play")
same(names(genres.query({ available = false }).items, "name"), "Jazz,Rock", "genres holding missing tracks")

-- The missing file's entry says since when; a missing track has no source to play.
local entry_lists = entries.by_track({ ids.playing, ids.missing })
same(entry_lists[ids.playing][1].missing_at_ms, nil, "a present entry")
assert(entry_lists[ids.missing][1].missing_at_ms ~= nil, "a missing entry has a time")
same(playback_sources.streamable(ids.missing), nil, "a missing track's source")
assert(playback_sources.streamable(ids.playing) ~= nil, "a playing track's source")
