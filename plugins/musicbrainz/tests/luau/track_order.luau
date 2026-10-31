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
