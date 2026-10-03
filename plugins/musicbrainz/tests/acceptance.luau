local acceptance = require("../resolution/acceptance")
local local_release = require("../resolution/local_release")

type TrackSpec = { title: string, recording: string, length: number? }

local CREDIT = { { name = "Artist", artist = { id = "artist", name = "Artist" } } }

local function release(id: string, title: string, tracks: { TrackSpec }): any
	local medium_tracks = {}
	for position, track in tracks do
		table.insert(medium_tracks, {
			title = track.title,
			position = position,
			length = track.length,
			recording = { id = track.recording },
		})
	end
	return {
		id = id,
		title = title,
		["artist-credit"] = CREDIT,
		media = { { position = 1, ["track-count"] = #medium_tracks, tracks = medium_tracks } },
	}
end

local function local_album(album: string?, titles: { string }, length: number?): local_release.LocalRelease
	local tracks = {}
	for number, title in titles do
		table.insert(tracks, { track_title = title, disc = 1, track = number, duration_ms = length })
	end
	return local_release.from_context({
		release_title = album,
		artists = { { artist_name = "Artist" } },
		tracks = tracks,
		library = { language = "eng", country = "XW" },
	} :: any)
end

local function accepted(candidate: any, info: local_release.LocalRelease, anchor: any?): boolean
	return (acceptance.accepts(candidate, info, anchor))
end

-- A recording verified as the local audio, like an AcoustID match, places a
-- single track on its album when the track's album tag names nothing.
local album_tracks: { TrackSpec } = {}
for number = 1, 10 do
	table.insert(album_tracks, { title = `Song {number}`, recording = `album-{number}`, length = 200000 })
end
local album = release("album", "The Album", album_tracks)
local fingerprinted = { ["album-3"] = true }

for _, tag in { "Unknown Album", "" } do
	local untagged = local_album(tag, { "Song 3" }, 200000)
	assert(not accepted(album, untagged), `album tag {tag}: rejected without evidence`)
	assert(
		accepted(album, local_release.with_verified_recordings(untagged, fingerprinted)),
		`album tag {tag}: accepted with a verified recording`
	)
end

-- An album tag that names something else, like the single the track came
-- from, still keeps the track off a larger release.
local single = local_album("Song 3", { "Song 3" }, 200000)
assert(not accepted(album, local_release.with_verified_recordings(single, fingerprinted)))

-- A translated edition shares the accepted edition's recordings but none of
-- its titles, and here no lengths either. The accepted edition's recordings
-- identify it.
local original = release("original", "Original", {
	{ title = "Morning", recording = "rec-1" },
	{ title = "Evening", recording = "rec-2" },
})
local translated = release("translated", "Translated", {
	{ title = "Asa", recording = "rec-1" },
	{ title = "Yoru", recording = "rec-2" },
})
local tracks = local_album("Original", { "Morning", "Evening" })
assert(accepted(original, tracks))
assert(not accepted(translated, tracks, original))
local verified =
	local_release.with_verified_recordings(tracks, acceptance.assigned_recording_ids(original, tracks.context.tracks))
assert(accepted(translated, verified, original))

-- Verified recordings only identify releases that carry them.
local other = release("other", "Other", {
	{ title = "Ichi", recording = "rec-3" },
	{ title = "Ni", recording = "rec-4" },
})
assert(not accepted(other, verified, original))
