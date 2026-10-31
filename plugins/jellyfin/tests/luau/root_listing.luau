local fixture = ...

local constants = require("../../constants")
local filters = require("../../protocol/items/filters")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- The root and the playlists view name no entity, so a listing under either is unscoped rather
-- than matching nothing as an id naming nothing does.

constants.configure_root_item_id(string.rep("1", 32))

local function parsed(params: { [string]: string })
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.listener)
end

local function items(params: { [string]: string }): string
	local listed, total = listing.list_items(parsed(params))
	local names = {}
	for _, item in listed do
		table.insert(names, `{item.Name}:{item.Type}`)
	end
	return `{table.concat(names, ",")} ({total})`
end

local function genres(params: { [string]: string }): string
	local json = filters.legacy_json(filters.collect(parsed(params))) :: any
	return table.concat(json.Genres, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

local root = constants.ROOT_ITEM_ID
check(
	items({ ParentId = root, Recursive = "true", IncludeItemTypes = "Audio" }),
	"S1:Audio (1)",
	"recursive tracks under the root"
)
check(
	items({ ParentId = root, Recursive = "true", IncludeItemTypes = "MusicAlbum", Filters = "IsUnplayed" }),
	"Solo:MusicAlbum (1)",
	"filtered albums under the root"
)
check(
	items({ ParentId = constants.EMPTY_ITEM_ID, Recursive = "true", IncludeItemTypes = "Audio" }),
	"S1:Audio (1)",
	"recursive tracks under the empty id, which names the root"
)
check(genres({ ParentId = root }), "Rock", "filters under the root")
check(genres({ ParentId = constants.PLAYLISTS_FOLDER_ID }), "Rock", "filters under the playlists view")

local unknown = item_ids.nanoid_to_hex("NoSuchItemAnywhere000") :: string
check(
	items({ ParentId = unknown, Recursive = "true", IncludeItemTypes = "Audio" }),
	" (0)",
	"an id naming nothing still matches nothing"
)
