local fixture = ...
local releases = require("@lyra/releases")

local ids = fixture.ids

-- A rejected set's error names the offending id by its public id, and carries no other number.
local function rejects(release_id: string, chains: { { string } }, message: string, id: string)
	local ok, err = pcall(releases.replace_chains, release_id, chains)
	assert(not ok, `rejected: {message}`)
	local text = tostring(err)
	assert(string.find(text, message, 1, true), text)
	local named = string.find(text, id, 1, true)
	assert(named, `names {id}: {text}`)
	local rest = string.sub(text, 1, named - 1) .. string.sub(text, named + #id)
	assert(not string.find(rest, "%d"), `no database id: {text}`)
end

rejects(ids.book, { { ids.first, ids.first } }, `track {ids.first} is listed more than once`, ids.first)
rejects(ids.book, { { ids.first, ids.elsewhere } }, `track {ids.elsewhere} is not on the release`, ids.elsewhere)
-- A track the caller can't see reads as one that doesn't exist, before the set's shape is checked.
rejects(ids.book, { { ids.hidden } }, `track {ids.hidden} is not on the release`, ids.hidden)
rejects(ids.book, { { "missing-track" } }, "track missing-track is not on the release", "missing-track")
rejects(ids.book, { { ids.first, ids.hidden } }, `track {ids.hidden} is not on the release`, ids.hidden)
rejects(ids.secret, { { ids.hidden, ids.hidden } }, `release not found: {ids.secret}`, ids.secret)
rejects("missing-release", {}, "release not found: missing-release", "missing-release")

assert(#releases.chains({ ids.book })[ids.book] == 0, "rejected sets store nothing")
