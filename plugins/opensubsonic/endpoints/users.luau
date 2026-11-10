--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local libraries = require("@lyra/libraries")
local users = require("@lyra/users")

local folders = require("../catalog/folders")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type Library = libraries.Library
type User = users.User
type Request = request.Request

-- Every user streams, rates, makes playlists, and gets cover art. Lyra records every user's
-- listens. Downloads need the download permission. Settings and passwords change in Lyra rather
-- than through this API, and Lyra has no uploads, comments, podcasts, jukebox, shares, or video.
local function user_item(user: User, visible: { Library }): JsonObject
	local permissions = user.permissions
	local folder_ids: { number } = {}
	for _, library in visible do
		table.insert(folder_ids, folders.id(library.id))
	end
	return {
		username = user.username,
		scrobblingEnabled = true,
		adminRole = request.holds(permissions, "admin"),
		settingsRole = false,
		downloadRole = request.holds(permissions, "download"),
		uploadRole = false,
		playlistRole = true,
		coverArtRole = true,
		commentRole = false,
		podcastRole = false,
		streamRole = true,
		jukeboxRole = false,
		shareRole = false,
		videoConversionRole = false,
		folder = folder_ids,
	}
end

local module = {}

module.methods = {
	-- Usernames match as sign-in matches them. Only an admin may name another user.
	getUser = function(req: Request): ApiResponse
		local caller = req.auth.principal
		local user = users.list({ username = request.required(req.params, "username") })[1]
		if not request.holds(caller.permissions, "admin") and not (user and user.id == caller.user_id) then
			return request.fail(50, "Only admins can read another user")
		end
		if not user then
			return request.not_found("User")
		end
		local visible = libraries.by_user({ user.id })[user.id]
		return response.ok(req.params, { user = user_item(user, visible) })
	end,

	getUsers = function(req: Request): ApiResponse
		if not request.holds(req.auth.principal.permissions, "admin") then
			return request.fail(50, "Only admins can list users")
		end
		local listed = users.list()
		local ids: { string } = {}
		for _, user in listed do
			table.insert(ids, user.id)
		end
		local visible = libraries.by_user(ids)
		local items: { JsonObject } = {}
		for _, user in listed do
			table.insert(items, user_item(user, visible[user.id]))
		end
		return response.ok(req.params, { users = { user = items } })
	end,
}

return module
