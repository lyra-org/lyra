local fixture = ...
local playlists = require("@lyra/playlists")

local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- Playlists sort as upstream sorts any item: by name, run time, creation date or at random, over
-- several keys, each in its own order.

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

check({}, "Alpha,Beta,Epsilon,Gamma", "by name without SortBy")
check({ SortOrder = "Descending" }, "Gamma,Epsilon,Beta,Alpha", "SortOrder alone reverses the names")
check({ SortBy = "SortName", SortOrder = "Descending" }, "Gamma,Epsilon,Beta,Alpha", "SortName")
check({ SortBy = "Name" }, "Alpha,Beta,Epsilon,Gamma", "Name")
check({ SortBy = "Runtime" }, "Beta,Epsilon,Gamma,Alpha", "Runtime, equal run times by name")
check({ SortBy = "Runtime", SortOrder = "Descending" }, "Alpha,Gamma,Beta,Epsilon", "Runtime descending")
check(
	{ SortBy = "Runtime,SortName", SortOrder = "Ascending,Descending" },
	"Epsilon,Beta,Gamma,Alpha",
	"each key takes its own order"
)
check({ SortBy = "PlayCount,CommunityRating" }, "Alpha,Beta,Epsilon,Gamma", "keys without a meaning are dropped")
check(
	{ SortBy = "Runtime", SortOrder = "Descending", StartIndex = "1", Limit = "2" },
	"Gamma,Beta",
	"the sort comes before the page"
)

-- A playlist made later is the newest.
local later = os.time() * 1000 + 60000
playlists.create({ name = "Delta", created_at_ms = later, updated_at_ms = later })
check({ SortBy = "DateCreated", SortOrder = "Descending", Limit = "1" }, "Delta", "DateCreated descending")
local oldest_first = names({ SortBy = "DateCreated,SortName" })
assert(string.sub(oldest_first, -6) == ",Delta", `DateCreated ascending ends with the newest, got "{oldest_first}"`)

-- Random draws a new order for every request, over the same playlists.
local orders: { [string]: boolean } = {}
local distinct = 0
for _ = 1, 30 do
	local order = names({ SortBy = "Random" })
	local sorted = string.split(order, ",")
	table.sort(sorted)
	assert(table.concat(sorted, ",") == "Alpha,Beta,Delta,Epsilon,Gamma", `random keeps every playlist, got "{order}"`)
	if not orders[order] then
		orders[order] = true
		distinct += 1
	end
end
assert(distinct > 1, "random orders differ between requests")
