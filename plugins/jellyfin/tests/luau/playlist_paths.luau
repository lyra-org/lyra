local fixture = ...
local constants = require("../../constants")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- As upstream, a playlist's path is its folder under `<DataPath>/playlists`, named after the
-- playlist with the characters a file name can't hold replaced by spaces. Clients such as Jellify
-- tell a server's own playlists apart by it.

local function first(params: { [string]: string }): any
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	local items = listing.list_items(options.for_user(options.parse(raw :: any), fixture.users.listener))
	assert(#items == 1, `one item, got {#items}`)
	return items[1]
end

constants.configure_root_item_id(string.rep("1", 32))
local folder = first({ IncludeItemTypes = "ManualPlaylistsFolder" })
assert(folder.Path == "/config/data/playlists", `the folder's path, got "{folder.Path}"`)
local playlist = first({ ParentId = constants.PLAYLISTS_FOLDER_ID })
local expected = "/config/data/playlists/Road Trip   Mix  "
assert(playlist.Path == expected, `the playlist's path, got "{playlist.Path}"`)
