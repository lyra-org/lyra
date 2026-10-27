local fixture = ...
local constants = require("../../constants")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- Upstream shows the playlists view only while the caller can see a playlist, but the folder
-- itself still answers a lookup by its type.

local function names(list_items: (options.ItemsQueryOptions) -> any, params: { [string]: string }): string
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	local items = list_items(options.for_user(options.parse(raw :: any), fixture.users.listener))
	local listed = {}
	for _, item in items do
		table.insert(listed, `{item.Name}:{item.Type}`)
	end
	return table.concat(listed, ",")
end

constants.configure_root_item_id(string.rep("1", 32))
local views = names(listing.list_views, {})
assert(views == "", `no playlists view without a visible playlist, got "{views}"`)
local folder = names(listing.list_items, { IncludeItemTypes = "ManualPlaylistsFolder" })
assert(folder == "Playlists:ManualPlaylistsFolder", `the folder by type, got "{folder}"`)
local children = names(listing.list_items, { ParentId = constants.PLAYLISTS_FOLDER_ID })
assert(children == "", `the private playlist doesn't list, got "{children}"`)
