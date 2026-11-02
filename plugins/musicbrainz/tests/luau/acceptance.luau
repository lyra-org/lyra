local acceptance = require("../../resolution/acceptance")
local local_release = require("../../resolution/local_release")

type TrackSpec = { title: string, recording: string, length: number?, video: boolean? }

local CREDIT = { { name = "Artist", artist = { id = "artist", name = "Artist" } } }

local function release(id: string, title: string, tracks: { TrackSpec }): any
	local medium_tracks = {}
	for position, track in tracks do
		table.insert(medium_tracks, {
			title = track.title,
			position = position,
			length = track.length,
			recording = { id = track.recording, video = track.video },
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

-- Local files are audio: a release whose matching track is a music video of
-- the song is not the local release, however well it fits otherwise.
local song = local_album("Song", { "Song" }, 200000)
assert(accepted(release("audio", "Song", { { title = "Song", recording = "audio", length = 200000 } }), song))
assert(
	not accepted(
		release("video", "Song", { { title = "Song", recording = "video", length = 200000, video = true } }),
		song
	),
	"a video recording is rejected"
)

-- A length a file can't really have, like 252 ms for its song, is no evidence
-- against the release; a real one that disagrees still is.
local truncated = local_album("A Woman", { "A Woman" }, 252)
local woman = release("woman", "A Woman", { { title = "A Woman", recording = "woman", length = 96000 } })
assert(accepted(woman, truncated), "an implausible local length is ignored")
assert(not accepted(woman, local_album("A Woman", { "A Woman" }, 60000)), "a real length that disagrees rejects")

-- Every paired track must agree with its file. One that is a different song,
-- with another title and length, rejects the release; a track whose title
-- matches is evidence enough, however MusicBrainz counts its length.
local function local_tracks_of(album: string, tracks: { { string | number } }): local_release.LocalRelease
	local local_tracks = {}
	for number, spec in tracks do
		table.insert(local_tracks, { track_title = spec[1], disc = 1, track = number, duration_ms = spec[2] })
	end
	return local_release.from_context({
		release_title = album,
		artists = { { artist_name = "Artist" } },
		tracks = local_tracks,
		library = { language = "eng", country = "XW" },
	} :: any)
end
local awake = local_tracks_of("Awake / Speed", { { "Awake (English ver.)", 245000 }, { "Speed", 189600 } })
local other_single = release("other-single", "Awake", {
	{ title = "Awake", recording = "awake-jp", length = 246000 },
	{ title = "Stained Brain", recording = "stained", length = 212100 },
})
assert(not accepted(other_single, awake), "a release whose second track is another song is rejected")
local right_single = release("right-single", "Awake / Speed", {
	{ title = "Awake (English ver.)", recording = "awake-en", length = 245000 },
	{ title = "Speed", recording = "speed", length = 189600 },
})
assert(accepted(right_single, awake))

local hidden = local_tracks_of("Hidden", { { "First", 180000 }, { "Last", 200900 } })
local with_hidden_track = release("hidden", "Hidden", {
	{ title = "First", recording = "first", length = 180000 },
	{ title = "Last", recording = "last", length = 450600 },
})
assert(accepted(with_hidden_track, hidden), "a matching title outweighs a length counting a hidden track")

-- How far a track's length may stray depends on what else ties it to its file.
local function pair(album: string, local_specs: { { string | number } }, mb_specs: { { string | number } }): boolean
	local mb_tracks = {}
	for index, spec in mb_specs do
		table.insert(mb_tracks, { title = spec[1] :: string, recording = `rec-{index}`, length = spec[2] :: number })
	end
	return accepted(release(album, album, mb_tracks), local_tracks_of(album, local_specs))
end
assert(
	not pair(
		"Versions",
		{ { "Song (Club Remix)", 420000 }, { "B [Live]", 480000 } },
		{ { "Song", 180000 }, { "B", 190000 } }
	),
	"a remix and a live take are other versions, held to the strict tolerance"
)
-- A last track a minute or more longer may carry a hidden track, but only
-- when something shows it is the file's song: its title, or its recording
-- verified. Otherwise it may as well be another song.
assert(
	not pair("Romaji", { { "Hajime", 180000 }, { "Saigo", 200900 } }, { { "始め", 180000 }, { "最後", 450600 } }),
	"a much longer last track of another title is no evidence of a hidden track"
)
assert(
	not pair(
		"Finale",
		{ { "First", 180000 }, { "Last", 180000 } },
		{ { "First", 180000 }, { "Unrelated Live Finale", 360000 } }
	),
	"a different closing song is rejected"
)
local romaji = release("romaji-verified", "Romaji", {
	{ title = "始め", recording = "hajime", length = 180000 },
	{ title = "最後", recording = "saigo", length = 450600 },
})
assert(
	accepted(
		romaji,
		local_release.with_verified_recordings(
			local_tracks_of("Romaji", { { "Hajime", 180000 }, { "Saigo", 200900 } }),
			{ saigo = true }
		)
	),
	"a verified recording carries its hidden track under another title"
)
assert(
	not pair("Intros", { { "Intro", 9000 }, { "Song", 200000 } }, { { "Intro", 120000 }, { "Song", 200000 } }),
	"a 9 s intro is not a 120 s one"
)
assert(
	not pair("Edits", { { "Song", 180000 }, { "Coda", 60000 } }, { { "Song", 330000 }, { "Coda", 60000 } }),
	"a radio edit is not the extended mix of the same title"
)
assert(
	pair("Sleeve", { { "Song", 223051 }, { "Coda", 60000 } }, { { "Song", 218000 }, { "Coda", 60000 } }),
	"a same-titled length off by sleeve rounding agrees"
)
