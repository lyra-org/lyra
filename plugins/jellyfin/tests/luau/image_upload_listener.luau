local fixture = ...
local covers = require("@lyra/covers")
local crypt = require("@harmony/crypt")
local images = require("../../routes/images")
local item_ids = require("../../protocol/items/identifiers")

-- Changing an album's image needs the manage_metadata permission, as upstream needs an admin.

local PNG =
	"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d\x49\x48\x44\x52\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90\x77\x53\xde\x00\x00\x00\x0c\x49\x44\x41\x54\x78\x9c\x63\x60\x60\x60\x00\x00\x00\x04\x00\x01\xf6\x17\x38\x55\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82"
local id = item_ids.encode_jellyfin_item_id(fixture.ids.album)

local function ctx(method: string, headers: { [string]: string }): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = method,
			path = `/Items/{id}/Images/Primary`,
			headers = headers,
			query = {},
			body_raw = if method == "POST" then crypt.base64.encode(PNG) else "",
		},
		params = { imageId = id },
	}
end

local signed_in = {
	authorization = `MediaBrowser Client="Feishin", Token="{fixture.api_keys.listener_key}"`,
	["content-type"] = "image/png",
}
assert(images.handle_set_primary_image(ctx("POST", signed_in)).status == 403)
assert(images.handle_delete_primary_image(ctx("DELETE", signed_in)).status == 403)
assert(images.handle_set_primary_image(ctx("POST", { ["content-type"] = "image/png" })).status == 401)
assert(covers.get({ fixture.ids.album })[fixture.ids.album].hash == "album")
