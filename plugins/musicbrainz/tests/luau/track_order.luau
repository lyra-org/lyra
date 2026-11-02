local track_helpers = require("../../resolution/track_helpers")

-- Tracks that share a position and title keep the order they were created in,
-- then their ids; one without a creation time goes last.
local function ordered(tracks: { track_helpers.LocalTrack }): string
	track_helpers.sort_tracks_for_refresh(tracks)
	local ids = {}
	for _, track in tracks do
		table.insert(ids, track.id or "?")
	end
	return table.concat(ids, " ")
end

local function track(id: string?, created_at_ms: number?): track_helpers.LocalTrack
	return { id = id, track_title = "Song", disc = 1, track = 1, created_at_ms = created_at_ms }
end

assert(ordered({ track("a", 20), track("b", 10) }) == "b a", "the earlier track goes first")
assert(ordered({ track("b", 10), track("a", 10) }) == "a b", "tracks created together go by id")
assert(ordered({ track("a", nil), track("b", 10) }) == "b a", "a track without a creation time goes last")
assert(ordered({ track("b", nil), track("a", nil) }) == "a b", "tracks without creation times go by id")
assert(ordered({ track(nil, 10), track("a", 10) }) == "a ?", "a track without an id goes last")
assert(
	ordered({ { id = "a", track_title = "Song", disc = 1, track = 2, created_at_ms = 1 }, track("b", 10) }) == "b a",
	"the position still comes first"
)

-- Discs ripped to folders without disc numbers repeat track numbers, so
-- positions place nothing: titles do, and length tells same-titled tracks
-- apart. A title MusicBrainz spells otherwise takes the slot left.
local function mb_track(title: string, position: number, length: number): any
	return { title = title, position = position, length = length, recording = { id = `{title}-{length}` } }
end
local two_discs: any = {
	media = {
		{
			position = 1,
			tracks = { mb_track("Intro", 1, 100000), mb_track("Middle", 2, 120000), mb_track("Song", 3, 200000) },
		},
		{ position = 2, tracks = { mb_track("Other", 1, 150000), mb_track("Song", 2, 250000) } },
	},
}
local ripped: { track_helpers.LocalTrack } = {
	{ id = "cd1-1", track_title = "Intrro", track = 1, duration_ms = 100000 },
	{ id = "cd1-2", track_title = "Middle", track = 2, duration_ms = 120000 },
	{ id = "cd1-3", track_title = "Song", track = 3, duration_ms = 200000 },
	{ id = "cd2-1", track_title = "Other", track = 1, duration_ms = 150000 },
	{ id = "cd2-2", track_title = "Song", track = 2, duration_ms = 250000 },
}
local sorted, assignments = track_helpers.assign_release_tracks(ripped, two_discs)
local placed: { [string]: string } = {}
for i, local_track in sorted do
	local assigned = assignments[i]
	placed[local_track.id :: string] = if assigned then `{assigned.disc}/{assigned.track}` else "none"
end
assert(placed["cd1-3"] == "1/3", `the first disc's song by its length, got {placed["cd1-3"]}`)
assert(placed["cd2-2"] == "2/2", `the second disc's song by its length, got {placed["cd2-2"]}`)
assert(placed["cd2-1"] == "2/1", `got {placed["cd2-1"]}`)
assert(placed["cd1-1"] == "1/1", `a misspelled title takes the slot left, got {placed["cd1-1"]}`)

-- A file whose length singles out its track takes it before one without a
-- length to go by, whichever comes first.
local intros: any = {
	media = {
		{ position = 1, tracks = { mb_track("Intro", 1, 100000) } },
		{ position = 2, tracks = { mb_track("Intro", 1, 200000) } },
	},
}
local unlengthed: { track_helpers.LocalTrack } = {
	{ id = "misread", track_title = "Intro", track = 1, duration_ms = 252, created_at_ms = 1 },
	{ id = "measured", track_title = "Intro", track = 1, duration_ms = 100000, created_at_ms = 2 },
}
local intro_sorted, intro_assignments = track_helpers.assign_release_tracks(unlengthed, intros)
local intro_placed: { [string]: string } = {}
for i, local_track in intro_sorted do
	local assigned = intro_assignments[i]
	intro_placed[local_track.id :: string] = if assigned then `{assigned.disc}/{assigned.track}` else "none"
end
assert(intro_placed.measured == "1/1", `the measured file takes its track, got {intro_placed.measured}`)
assert(intro_placed.misread == "2/1", `the misread file takes the one left, got {intro_placed.misread}`)

local function placements(local_tracks: { track_helpers.LocalTrack }, release: any): { [string]: string }
	local sorted_tracks, paired = track_helpers.assign_release_tracks(local_tracks, release)
	local result: { [string]: string } = {}
	for i, local_track in sorted_tracks do
		local assigned = paired[i]
		result[local_track.id :: string] = if assigned then `{assigned.disc}/{assigned.track}` else "none"
	end
	return result
end

-- On a release of one medium, track numbers place tracks even without disc
-- tags, before lengths do.
local one_medium: any = {
	media = { { position = 1, tracks = { mb_track("Song", 1, 187000), mb_track("Song", 2, 180000) } } },
}
local numbered = placements({
	{ id = "first", track_title = "Song", track = 1, duration_ms = 180000 },
	{ id = "second", track_title = "Song", track = 2, duration_ms = 185000 },
}, one_medium)
assert(
	numbered.first == "1/1" and numbered.second == "1/2",
	`numbers place them, got {numbered.first} {numbered.second}`
)

-- Without numbers, tracks of a repeated title are told apart by length.
local unnumbered = placements({
	{ id = "short", track_title = "Song", duration_ms = 180000, created_at_ms = 1 },
	{ id = "long", track_title = "Song", duration_ms = 240000, created_at_ms = 2 },
}, { media = { { position = 1, tracks = { mb_track("Song", 1, 240000), mb_track("Song", 2, 180000) } } } } :: any)
assert(
	unnumbered.long == "1/1" and unnumbered.short == "1/2",
	`lengths place them, got {unnumbered.long} {unnumbered.short}`
)

-- A title naming a track places it, whatever its number says: files of a
-- second disc, numbered from one without disc tags, keep to their songs.
local second_disc = placements(
	{
		{ id = "b", track_title = "B", track = 1, duration_ms = 200000 },
		{ id = "c", track_title = "C", track = 2, duration_ms = 200000 },
	},
	{
		media = {
			{
				position = 1,
				tracks = {
					mb_track("A", 1, 200000),
					mb_track("B", 2, 200000),
					mb_track("C", 3, 200000),
					mb_track("D", 4, 200000),
				},
			},
		},
	} :: any
)
assert(second_disc.b == "1/2" and second_disc.c == "1/3", `titles place them, got {second_disc.b} {second_disc.c}`)

-- Numbers can't be trusted on discs ripped without disc tags, so they don't
-- tell same-titled tracks apart there: a second disc's Intros, numbered on from
-- the first disc, go by their lengths.
local continuous = placements(
	{
		{ id = "a", track_title = "A", track = 1, duration_ms = 100000 },
		{ id = "b", track_title = "B", track = 2, duration_ms = 100000 },
		{ id = "short", track_title = "Intro", track = 3, duration_ms = 200000 },
		{ id = "x", track_title = "X", track = 4, duration_ms = 100000 },
		{ id = "long", track_title = "Intro", track = 5, duration_ms = 300000 },
	},
	{
		media = {
			{ position = 1, tracks = { mb_track("A", 1, 100000), mb_track("B", 2, 100000) } },
			{
				position = 2,
				tracks = { mb_track("Intro", 1, 200000), mb_track("X", 2, 100000), mb_track("Intro", 3, 300000) },
			},
		},
	} :: any
)
assert(
	continuous.short == "2/1" and continuous.long == "2/3",
	`lengths place them, got {continuous.short} {continuous.long}`
)
