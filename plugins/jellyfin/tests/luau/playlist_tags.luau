local fixture = ...

local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- `Tags` keeps the playlists the caller tagged with any of its names, ignoring case.

local function names(params: { [string]: string }): string
	local raw: { [string]: { string } } = { IncludeItemTypes = { "Playlist" }, Recursive = { "true" } }
	for key, value in params do
		raw[key] = { value }
	end
	local items = listing.list_items(options.for_user(options.parse(raw :: any), fixture.users.listener))
	local listed = {}
	for _, item in items do
		table.insert(listed, item.Name)
	end
	return table.concat(listed, ",")
end

local function check(params: { [string]: string }, expected: string, why: string)
	local actual = names(params)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

check({}, "Alpha,Beta,Delta,Gamma", "every playlist without Tags")
check({ Tags = "VINYL" }, "Alpha,Beta", "a tag of any case, and only the caller's")
check({ Tags = "live|nope" }, "Gamma", "any of several tags")
check({ Tags = "Nope" }, "", "a tag the caller lacks")
