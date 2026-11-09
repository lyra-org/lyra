local fixture = ...
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local preferences = require("../../protocol/preferences")

local user_id = fixture.users.admin

local zed_library = item_ids.encode(fixture.libraries.zed) :: string
local alpha_library = item_ids.encode(fixture.libraries.alpha) :: string
constants.configure_root_item_id(string.rep("1", 32))
local playlists = constants.PLAYLISTS_FOLDER_ID

local function configure(body: { [string]: any })
	local ctx = { request = { json = body, body_raw = "{}" } }
	assert(preferences.user_configuration_set(ctx :: any, user_id).status == 204, "the configuration saves")
end

local function names(items: { any }): string
	local listed = {}
	for _, item in items do
		table.insert(listed, item.Name)
	end
	return table.concat(listed, ",")
end

local function parse(params: { [string]: string }?): options.ItemsQueryOptions
	local raw: { [string]: { string } } = {}
	for key, value in params or {} do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), user_id)
end

local function views(include_hidden: boolean?, params: { [string]: string }?): string
	return names((listing.list_views(parse(params), include_hidden)))
end

local function latest(params: { [string]: string }?): string
	return names(listing.list_latest_items(parse(params)))
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

check(views(), "Alpha Library,Playlists,Zed Library", "views sort by name by default")
check(latest(), "Amy New,Bob Mid,Amy Old", "the newest albums of every library")

configure({ OrderedViews = { zed_library, playlists } })
check(views(), "Zed Library,Playlists,Alpha Library", "the caller's order comes first, then the name")
check(
	views(nil, { SortBy = "SortName", SortOrder = "Descending" }),
	"Zed Library,Playlists,Alpha Library",
	"a listing's SortBy replaces the caller's order"
)
configure({ OrderedViews = { alpha_library } })
check(
	views(nil, { SortBy = "SortName", SortOrder = "Descending" }),
	"Zed Library,Playlists,Alpha Library",
	"whatever the caller's order"
)

configure({ MyMediaExcludes = { playlists, alpha_library } })
check(views(), "Zed Library", "views hidden from the caller's media don't show")
check(views(true), "Alpha Library,Playlists,Zed Library", "unless hidden views are asked for")
check(latest(), "Amy New,Bob Mid,Amy Old", "hiding a view keeps its latest items")

configure({ LatestItemsExcludes = { zed_library } })
check(latest(), "Bob Mid", "a library excluded from latest media drops out")
check(views(), "Alpha Library,Playlists,Zed Library", "and still shows as a view")
check(latest({ ParentId = zed_library }), "Amy New,Amy Old", "a parent's latest items ignore the excludes")

configure({ LatestItemsExcludes = { alpha_library } })
check(latest({ Limit = "1" }), "Amy New", "the newest of the kept libraries")

configure({ LatestItemsExcludes = { zed_library, alpha_library } })
check(latest(), "", "with every library excluded nothing is latest")

configure({ HidePlayedInLatest = false })
check(latest(), "Amy New,Bob Mid,Amy Old", "played music shows in latest either way")
