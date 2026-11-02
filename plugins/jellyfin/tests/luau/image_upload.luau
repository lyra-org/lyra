local fixture = ...
local covers = require("@lyra/covers")
local crypt = require("@harmony/crypt")
local images = require("../../routes/images")
local item_ids = require("../../protocol/items/identifiers")

-- Feishin uploads an artist's or playlist's image as Jellyfin's web client does: the image,
-- base64-encoded, posted to the item's primary image with its Content-Type, and deletes it by the
-- same path.

local PNG =
	"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d\x49\x48\x44\x52\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90\x77\x53\xde\x00\x00\x00\x0c\x49\x44\x41\x54\x78\x9c\x63\x60\x60\x60\x00\x00\x00\x04\x00\x01\xf6\x17\x38\x55\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82"
local ids = fixture.ids

local function request(method: string, id: string, body: string?, content_type: string?): number
	local headers: { [string]: string } = {
		authorization = `MediaBrowser Client="Feishin", Device="Firefox", DeviceId="feishin", Version="0.12.0", Token="{fixture.api_keys.editor_key}"`,
	}
	if content_type then
		headers["content-type"] = content_type
	end
	local ctx = {
		plugin_id = "jellyfin",
		request = {
			method = method,
			path = `/Items/{id}/Images/Primary`,
			headers = headers,
			query = {},
			body_raw = body or "",
		},
		params = { imageId = id },
	} :: any
	local handle = if method == "POST" then images.handle_set_primary_image else images.handle_delete_primary_image
	return handle(ctx).status
end

local function upload(id: string, image: string?): number
	return request(
		"POST",
		item_ids.encode_jellyfin_item_id(id) :: string,
		crypt.base64.encode(image or PNG),
		"image/png"
	)
end

local function delete(id: string): number
	return request("DELETE", item_ids.encode_jellyfin_item_id(id) :: string)
end

local function hash(id: string): string?
	local cover = covers.get({ id })[id]
	return if cover then cover.hash else nil
end

-- An artist without a cover takes the upload, and loses it again.
assert(hash(ids.singer) == nil)
assert(upload(ids.singer) == 204)
assert(hash(ids.singer) ~= nil, "the artist shows the uploaded image")
assert(delete(ids.singer) == 204)
assert(hash(ids.singer) == nil)
assert(delete(ids.singer) == 204, "like upstream, deleting an image the item lacks succeeds")

-- An album's upload replaces its own cover until it is deleted.
assert(upload(ids.album) == 204)
assert(hash(ids.album) ~= "album")
assert(delete(ids.album) == 204)
assert(hash(ids.album) == "album", "the album's own cover shows again")

-- The caller's playlist takes one; another user's does not.
assert(upload(ids.mine) == 204)
assert(covers.get({ ids.mine })[ids.mine].owner_id == ids.mine)
assert(delete(ids.mine) == 204)
assert(upload(ids.theirs) == 403, "only the owner changes a playlist's image")
assert(delete(ids.theirs) == 403)
assert(upload(ids.private) == 404, "another user's private playlist is missing")

-- Missing items, items without an image of their own, and bad bodies.
assert(upload(string.rep("a", 21)) == 404)
assert(upload(ids.song) == 404, "a track shows its album's image")
assert(request("POST", "not-an-item-id", crypt.base64.encode(PNG), "image/png") == 404)
local album = item_ids.encode_jellyfin_item_id(ids.album) :: string
assert(request("POST", album, crypt.base64.encode(PNG), "application/json") == 400, "the Content-Type must be an image")
assert(request("POST", album, crypt.base64.encode(PNG)) == 400, "a Content-Type is required")
assert(request("POST", album, "not base64!", "image/png") == 400)
assert(upload(ids.album, "not an image") == 400)
assert(hash(ids.album) == "album", "nothing was stored")

-- Upstream's decoder skips whitespace, as a line-wrapped body has.
local encoded = crypt.base64.encode(PNG)
local wrapped = string.sub(encoded, 1, 40) .. "\r\n" .. string.sub(encoded, 41)
assert(request("POST", album, wrapped, "image/png; charset=utf-8") == 204)
assert(hash(ids.album) ~= "album")
