local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local playlist_access = require("../../protocol/playlist_access")
local session = require("../../protocol/session")

-- A user who isn't an admin can't delete another user's playlist, even a public one.

local response = session.handle_delete_item_route({
	plugin_id = "jellyfin",
	request = {
		method = "DELETE",
		path = "/Items",
		headers = { authorization = `MediaBrowser Token="{fixture.api_keys.listener_key}"` },
		query = {},
		body_raw = "",
		json = nil,
	},
	params = { id = item_ids.encode_jellyfin_item_id(fixture.ids.shared) },
} :: any)
assert(response.status == 401, `unauthorized, as upstream, got {response.status}`)
assert(playlist_access.find(fixture.ids.shared) ~= nil, "and the playlist is kept")
