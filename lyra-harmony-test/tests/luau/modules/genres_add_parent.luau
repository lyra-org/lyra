local fixture = ...
local genres = require("@lyra/genres")

local ids = fixture.ids

local function rejects(child_id: string, parent_id: string, message: string)
	local ok, err = pcall(genres.add_parent, child_id, parent_id)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

rejects(ids.album, ids.rock, `child_id {ids.album} is not a genre`)
rejects(ids.punk, ids.song, `parent_id {ids.song} is not a genre`)
rejects("missing-genre", ids.rock, "child_id missing-genre is not a genre")

-- Two genres link, and linking them again is harmless.
genres.add_parent(ids.punk, ids.rock)
genres.add_parent(ids.punk, ids.rock)
