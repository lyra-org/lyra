local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local playlist_access = require("../../protocol/playlist_access")
local session = require("../../protocol/session")

-- As upstream, an admin deletes any playlist it can see; another user's private one isn't found.

local function delete(id: number): number
	local response = session.handle_delete_item_route({
		plugin_id = "jellyfin",
		request = {
			method = "DELETE",
			path = "/Items",
			headers = { authorization = `MediaBrowser Token="{fixture.api_keys.admin_key}"` },
			query = {},
			body_raw = "",
			json = nil,
		},
		params = { id = item_ids.encode_jellyfin_item_id(id) },
	} :: any)
	return response.status
end

local ids = fixture.ids
local private = delete(ids.mix)
assert(private == 404, `another user's private playlist isn't found, got {private}`)
assert(playlist_access.find(ids.mix) ~= nil, "and it is kept")
local shared = delete(ids.shared)
assert(shared == 204, `an admin deletes another user's public playlist, got {shared}`)
assert(playlist_access.find(ids.shared) == nil, "and it is gone")
local own = delete(ids.own)
assert(own == 204, `an admin deletes its own playlist, got {own}`)
