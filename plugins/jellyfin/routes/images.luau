--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local covers = require("@lyra/covers")
local entities = require("@lyra/entities")

local crypt = require("@harmony/crypt")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local playlist_access = require("../protocol/playlist_access")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type Principal = session.Principal

local module = {}

-- The item whose primary image the request changes, once the caller may change it: a release or
-- artist with the manage_metadata permission, or a playlist the caller owns. Lyra's own check
-- stands; this one only tells a missing permission apart from a bad image.
local function resolve_item(ctx: ApiContext): (string?, ApiResponse?)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return nil, session.unauthorized()
	end
	local item_id = item_ids.decode_jellyfin_item_id(ctx.params.imageId)
	if not item_id then
		return nil, responses.not_found("item not found")
	end
	local item_type = entities.get_type(item_id)
	if item_type == "Release" or item_type == "Artist" then
		if not session.has_permission(principal, "manage_metadata") then
			return nil, responses.forbidden("changing an item's image requires the manage_metadata permission")
		end
	elseif item_type == "Playlist" then
		if not playlist_access.can_edit(playlist_access.find(item_id), principal.user_id) then
			return nil, responses.forbidden("only the playlist's owner may change its image")
		end
	else
		return nil, responses.not_found("item not found")
	end
	return item_id, nil
end

-- Upstream's body is the image, base64-encoded, with an image Content-Type.
local function decode_image(ctx: ApiContext): (string?, ApiResponse?)
	local content_type = string.lower(ctx.request.headers["content-type"] or "")
	if string.sub(content_type, 1, 6) ~= "image/" then
		return nil, responses.bad_request("Incorrect ContentType.")
	end
	local encoded = string.gsub(ctx.request.body_raw, "%s", "")
	local ok, image = pcall(crypt.base64.decode, encoded)
	if encoded == "" or not ok then
		return nil, responses.bad_request("the body is not a base64-encoded image")
	end
	return image, nil
end

function module.handle_set_primary_image(ctx: ApiContext): ApiResponse
	local item_id, item_err = resolve_item(ctx)
	if not item_id then
		return item_err or responses.not_found("item not found")
	end
	local image, image_err = decode_image(ctx)
	if not image then
		return image_err or responses.bad_request()
	end
	local ok, stored = pcall(covers.set_manual, item_id, image)
	if not ok then
		return responses.bad_request(tostring(stored))
	end
	if not stored then
		return responses.not_found("item not found")
	end
	return api.response.empty(204)
end

-- Removes the image uploaded for the item; like upstream, an item without one answers 204 too.
function module.handle_delete_primary_image(ctx: ApiContext): ApiResponse
	local item_id, item_err = resolve_item(ctx)
	if not item_id then
		return item_err or responses.not_found("item not found")
	end
	local ok = pcall(covers.remove_manual, item_id)
	if not ok then
		return api.response.empty(500)
	end
	return api.response.empty(204)
end

-- Clients send the image base64-encoded, a third larger than the image, and may wrap it in lines
-- of 76 characters ending in CRLF, as MIME does.
local MAX_BASE64_BYTES = 4 * math.ceil(covers.MAX_IMAGE_BYTES / 3)
local MAX_UPLOAD_BODY_BYTES = MAX_BASE64_BYTES + 2 * math.ceil(MAX_BASE64_BYTES / 76)

function module.register()
	api.post(P .. "/Items/{imageId}/Images/Primary", module.handle_set_primary_image, {
		auth = "public",
		case_insensitive = true,
		max_body_bytes = MAX_UPLOAD_BODY_BYTES,
	})
	api.delete(
		P .. "/Items/{imageId}/Images/Primary",
		module.handle_delete_primary_image,
		{ auth = "public", case_insensitive = true }
	)
end

return module
