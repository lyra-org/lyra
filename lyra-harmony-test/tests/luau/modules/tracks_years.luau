local fixture = ...
local tracks = require("@lyra/tracks")

local ids = fixture.ids

local function titles(query: tracks.TrackQuery): string
	query.sort = { { key = "year" } }
	local names = {}
	for _, track in tracks.query(query).items do
		table.insert(names, track.track_title)
	end
	return table.concat(names, ",")
end

local function same(actual: string, expected: string, label: string)
	assert(actual == expected, `{label}: expected {expected}, got {actual}`)
end

local function rejects(message: string, query: tracks.TrackQuery)
	local ok, err = pcall(tracks.query, query)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

-- Both bounds are inclusive, either may be left open, and any bound leaves out undated tracks.
same(titles({}), "Early,Middle,Late,Undated", "unbounded")
same(titles({ min_year = 2000 }), "Middle,Late", "min_year")
same(titles({ max_year = 2000 }), "Early,Middle", "max_year")
same(titles({ min_year = 1990, max_year = 2000 }), "Early,Middle", "both")
same(titles({ min_year = 2000, max_year = 2000 }), "Middle", "a single year")
same(titles({ min_year = 2011 }), "", "past every track")
same(titles({ min_year = 0 }), "Early,Middle,Late", "every dated track")
-- The bounds narrow `years` rather than replacing it.
same(titles({ years = { 1990, 2010 }, min_year = 2000 }), "Late", "with years")
assert(tracks.query({ max_year = 2005, limit = 1 }).total == 2, "the total counts within the bounds")
assert(tracks.query({ ids = { ids.undated }, min_year = 0 }).total == 0, "an undated track by id")

rejects("min_year must not exceed max_year", { min_year = 2001, max_year = 2000 })
rejects("min_year must be a year from 0 to 4294967295, got -1", { min_year = -1 })
rejects("max_year must be a year from 0 to 4294967295, got -5", { max_year = -5 })
