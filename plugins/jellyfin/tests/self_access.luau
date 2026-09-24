local playlists = require("@lyra/playlists")

local item_ids = require("../protocol/items/identifiers")
local playlist_access = require("../protocol/playlist_access")
local preferences = require("../protocol/preferences")
local session = require("../protocol/session")

local function context(query, params, json)
	return {
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/",
			headers = {},
			query = query,
			body_raw = if json then "{}" else "",
			json = json,
		},
		params = params,
	}
end

-- The dispatch principal owns the playlists it creates; its user id is its public id.
local playlist_id = playlists.create({ name = "Mine" })
local playlist = playlists.get_by_id(playlist_id)
assert(playlist ~= nil)
local self_id = playlist_access.owner_id(playlist)
assert(type(self_id) == "string" and #self_id == 21)
local other_id = string.rep("x", 21)

-- Own playlist edit.
assert(playlist_access.can_edit(playlist, self_id))
assert(playlist_access.can_view(playlist, self_id))
assert(not playlist_access.can_edit(playlist, other_id))
assert(not playlist_access.can_view(playlist, other_id))
local owned = playlist_access.list_owned()
assert(#owned == 1 and owned[1].db_id == playlist_id)
local renamed = playlists.update({ playlist_id = playlist_id, name = "Renamed" })
assert(renamed ~= nil and renamed.name == "Renamed")
local permission = session.to_playlist_user_permission(playlist, self_id)
assert(permission.CanEdit == true)
assert(permission.UserId == item_ids.encode_jellyfin_item_id(self_id))

-- `/Users/{self}`-style routes accept the principal's own Jellyfin id and nothing else.
local principal = { user_id = self_id, username = "luau-tester", role = nil, permissions = {} }
local self_hex = item_ids.encode_jellyfin_item_id(self_id)
local other_hex = item_ids.encode_jellyfin_item_id(other_id)
assert(self_hex ~= nil and other_hex ~= nil)
assert(item_ids.decode_jellyfin_user_id(self_hex) == self_id)
assert(item_ids.decode_jellyfin_user_id("not-a-jellyfin-id") == nil)
assert(session.require_matching_user_scope(context({}, { userId = self_hex }), principal))
assert(not session.require_matching_user_scope(context({}, { userId = other_hex }), principal))
assert(session.require_matching_user_scope(context({ userId = { self_hex } }, {}), principal))
assert(not session.require_matching_user_scope(context({ userId = { other_hex } }, {}), principal))
assert(session.ensure_user_scope_matches_principal(self_hex, principal))
assert(not session.ensure_user_scope_matches_principal(other_hex, principal))

-- Display preferences are keyed by the user's public id.
local query = { client = { "web" } }
local stored = preferences.display_prefs_set(context(query, {}, { SortBy = "Album" }), self_id, "usersettings")
assert(stored.status == 204)
local mine = preferences.display_prefs_get(context(query, {}), self_id, "usersettings")
assert(mine.kind == "json" and (mine.body :: any).SortBy == "Album")
local theirs = preferences.display_prefs_get(context(query, {}), other_id, "usersettings")
assert(theirs.kind == "json" and (theirs.body :: any).SortBy == "SortName")
