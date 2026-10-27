local fixture = ...
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local lookups = require("../../protocol/items/lookups")
local options = require("../../protocol/items/options")

-- The same catalog as `listing`, seen by the admin who owns none of its playlists.

local ids = fixture.ids

local function hex(id: number): string
	return item_ids.encode_jellyfin_item_id(id)
end

-- `Name:Type` for each item `/Items` returns, then its total.
local function items(params: { [string]: string }, parent: number?): string
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	if parent then
		raw.ParentId = { hex(parent) }
	end
	local listed, total = listing.list_items(options.for_user(options.parse(raw :: any), fixture.users.other))
	local names = {}
	for _, item in listed do
		table.insert(names, `{item.Name}:{item.Type}`)
	end
	return `{table.concat(names, ",")} ({total})`
end

local function check(actual: string, expected: string, why: string?)
	assert(actual == expected, `{why or "listing"}: expected "{expected}", got "{actual}"`)
end

-- Another user's playlist lists only when it is public.
check(items({}, ids.mix), " (0)")
check(items({}, ids.shared), "S1:Audio (1)")

check(
	items({ Ids = hex(ids.shared), IncludeItemTypes = "Playlist", Filters = "IsUnplayed" }),
	"Shared:Playlist (1)",
	"played state is the caller's own"
)

-- An admin sees another user's playlist only when it is public, as upstream, by any path.
check(items({ Ids = hex(ids.mix), IncludeItemTypes = "Playlist" }), " (0)", "a private playlist by Ids")
check(
	items({ Ids = `{hex(ids.mix)},{hex(ids.shared)}`, IncludeItemTypes = "Playlist", SortBy = "SortName" }),
	"Shared:Playlist (1)"
)
check(items({ IncludeItemTypes = "Playlist", Recursive = "true" }), "Shared:Playlist (1)")
assert(lookups.visible_kind(ids.mix, fixture.users.other) == nil, "a private playlist seeds no mix")
assert(lookups.visible_kind(ids.shared, fixture.users.other) == "Playlist")

constants.configure_root_item_id(string.rep("1", 32))
check(items({ ParentId = constants.PLAYLISTS_FOLDER_ID }), "Shared:Playlist (1)", "the playlists view")
