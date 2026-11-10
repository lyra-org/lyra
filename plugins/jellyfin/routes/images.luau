--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local covers = require("@lyra/covers")
local entities = require("@lyra/entities")

local crypt = require("@harmony/crypt")

local artwork = require("../protocol/items/images")
local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local lookups = require("../protocol/items/lookups")
local playlist_access = require("../protocol/playlist_access")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")
local statics = require("../protocol/statics")

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
	local item_id = item_ids.decode(ctx.params.imageId)
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

-- The item's images, as far as Lyra knows them: its one primary image, without the file's size
-- or dimensions. The caller must be signed in, and the plugin's own folders have no image.
function module.handle_item_image_infos(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end
	local raw_id = ctx.params.imageId
	if constants.is_root_item_id(raw_id) or constants.is_playlists_folder_id(raw_id) then
		return api.response.json(200, {})
	end
	local item_id = item_ids.decode(raw_id)
	local kind = if item_id then lookups.visible_kind(item_id, principal.user_id) else nil
	if not item_id or not kind or not lookups.ITEM_KINDS[kind] then
		return responses.not_found("item not found")
	end
	local cover = artwork.get_item_cover_file(item_id)
	if not cover then
		return api.response.json(200, {})
	end
	return api.response.json(
		200,
		{
			{
				ImageType = "Primary",
				ImageTag = cover.hash,
				Size = 0,
				BlurHash = cover.blurhash,
			},
		} :: any
	)
end

local PUBLIC = { auth = "public", case_insensitive = true }

local function get_and_head(path: string, handler: (ApiContext) -> ApiResponse)
	api.get(P .. path, handler, PUBLIC :: any)
	api.head(P .. path, handler, PUBLIC :: any)
end

-- Lyra has no artwork of these types, and no studios, so upstream would answer 404 to anyone: for
-- an unknown item, one the caller can't see, or one without the image.
local function handle_missing_image(_ctx: ApiContext): ApiResponse
	return statics.image_not_found_response()
end

local NON_PRIMARY_IMAGE_TYPES = {
	"Backdrop",
	"Logo",
	"Thumb",
	"Art",
	"Banner",
	"Disc",
	"Box",
	"BoxRear",
	"Menu",
	"Chapter",
	"Screenshot",
	"Profile",
}

local IMAGE_PATH_VALUES = "/{imageIndex}/{tag}/{format}/{maxWidth}/{maxHeight}/{percentPlayed}/{unplayedCount}"

function module.register()
	get_and_head("/Items/{imageId}/Images/Primary", session.handle_primary_image_request)
	get_and_head("/Items/{imageId}/Images/Primary/{imageIndex}", session.handle_primary_image_request)
	get_and_head("/Items/{imageId}/Images/Primary" .. IMAGE_PATH_VALUES, session.handle_primary_image_path_request)
	for _, image_type in NON_PRIMARY_IMAGE_TYPES do
		local path = "/Items/{imageId}/Images/" .. image_type
		get_and_head(path, handle_missing_image)
		get_and_head(path .. "/{imageIndex}", handle_missing_image)
		get_and_head(path .. IMAGE_PATH_VALUES, handle_missing_image)
	end
	get_and_head("/Studios/{name}/Images/{imageType}", handle_missing_image)
	get_and_head("/Studios/{name}/Images/{imageType}/{imageIndex}", handle_missing_image)
	get_and_head("/UserImage", session.handle_user_image_request)
	get_and_head("/Users/{userId}/Images/{imageType}", session.handle_user_images_route_request)
	get_and_head("/Users/{userId}/Images/{imageType}/{imageIndex}", session.handle_user_images_route_request)
	api.get(P .. "/Items/{imageId}/Images", module.handle_item_image_infos, PUBLIC :: any)
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
