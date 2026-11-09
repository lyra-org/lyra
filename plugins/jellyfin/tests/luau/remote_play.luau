local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")

local ids = fixture.ids
local names: { [string]: string } = {}
for key, id in ids do
	names[item_ids.encode(id) :: string] = key
end

local function hex(key: string): string
	return item_ids.encode(ids[key]) :: string
end

-- The fixture keys of what a remote play of `keys` queues, then the command it sends on.
local function play(keys: { string }, command: string?): string
	local requested = {}
	for _, key in keys do
		table.insert(requested, if ids[key] then hex(key) else key)
	end
	local queue, sent = listing.remote_play_queue(requested, command or "PlayNow", fixture.users.owner)
	local queued = {}
	for _, id in queue do
		table.insert(queued, names[id] or id)
	end
	return `{table.concat(queued, ",")} {sent}`
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

check(play({ "zed" }), "zed PlayNow", "a track plays itself")
check(play({ "beta" }), "one,two,three PlayNow", "an album plays its tracks by disc and number")
check(play({ "amy" }), "one,zed,two,three PlayNow", "an artist plays its tracks by SortName, as upstream")
check(play({ "rock" }), "one,two,three PlayNow", "a genre plays its tracks")
check(play({ "mine" }), "zed,two PlayNow", "a playlist plays its tracks once each, by SortName")
check(play({ "beta", "zed" }, "PlayNext"), "one,two,three,zed PlayNext", "items expand in request order")
check(play({ "secret" }), " PlayNow", "a playlist the caller can't see plays nothing")
check(play({ "00000000000000000000000000000000", "zed" }), "zed PlayNow", "unknown items drop")

local shuffled = string.split(play({ "beta" }, "PlayShuffle"), " ")
check(shuffled[2], "PlayNow", "a shuffle plays now")
local sorted = string.split(shuffled[1], ",")
table.sort(sorted)
check(table.concat(sorted, ","), "one,three,two", "a shuffle plays every track")

check(string.split(play({ "beta" }, "PlayInstantMix"), " ")[2], "PlayNow", "an instant mix plays now")
