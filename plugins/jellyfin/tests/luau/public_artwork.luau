local fixture = ...
local covers = require("@lyra/covers")

local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

local ids = fixture.ids

local function primary_image(id: number, api_key: string?)
	return session.handle_primary_image_request({
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/",
			headers = {},
			query = if api_key then { api_key = { api_key } } else {},
			body_raw = "",
		},
		params = { imageId = item_ids.encode_jellyfin_item_id(id) },
	} :: any)
end

local function status(id: number, api_key: string?): number
	return primary_image(id, api_key).status
end

-- Artwork routes are public, as upstream: an anonymous request gets any item's image, even a
-- private playlist's collage the viewer's own reads can't see.
assert(#covers.get_playlist_sources(ids.private) == 0)
assert(status(ids.private) == 200, `anonymous playlist image status {status(ids.private)}`)
assert(status(ids.album) == 200, `anonymous release image status {status(ids.album)}`)

-- With credentials, only what the caller can see; invalid credentials read as none.
local viewer = fixture.api_keys.phone
assert(status(ids.private, viewer) == 404, `viewer playlist image status {status(ids.private, viewer)}`)
assert(status(ids.album, viewer) == 404, `viewer release image status {status(ids.album, viewer)}`)
local owner = fixture.api_keys.desk
assert(status(ids.private, owner) == 200, `owner playlist image status {status(ids.private, owner)}`)
assert(status(ids.album, owner) == 200, `owner release image status {status(ids.album, owner)}`)
assert(status(ids.private, "invalid") == 200, `invalid-key playlist image status {status(ids.private, "invalid")}`)
