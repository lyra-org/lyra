--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local users = require("@lyra/users")

local folders = require("../catalog/folders")
local request = require("../protocol/request")
local response = require("../protocol/response")

type ApiResponse = api.ApiResponse
type JsonObject = { [string]: any }
type PublicUser = users.PublicUser
type Request = request.Request

-- Every user streams, rates, makes playlists, and gets cover art. Lyra records every user's
-- listens. Downloads need the download permission. Settings and passwords change in Lyra rather
-- than through this API, and Lyra has no uploads, comments, podcasts, jukebox, shares, or video.
local function user_item(user: PublicUser): JsonObject
	local permissions = user.permissions
	local folder_ids: { number } = {}
	for _, library_id in user.library_ids do
		table.insert(folder_ids, folders.id(library_id))
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
		local wanted = string.lower(request.required(req.params, "username"))
		if wanted ~= string.lower(caller.username) and not request.holds(caller.permissions, "admin") then
			return request.fail(50, "Only admins can read another user")
		end
		for _, user in users.list() do
			if string.lower(user.username) == wanted then
				return response.ok(req.params, { user = user_item(user) })
			end
		end
		return request.not_found("User")
	end,

	getUsers = function(req: Request): ApiResponse
		if not request.holds(req.auth.principal.permissions, "admin") then
			return request.fail(50, "Only admins can list users")
		end
		local items: { JsonObject } = {}
		for _, user in users.list() do
			table.insert(items, user_item(user))
		end
		return response.ok(req.params, { users = { user = items } })
	end,
}

return module
