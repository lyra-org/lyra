local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local session = require("../../protocol/session")

local ids = fixture.ids

local function image_status(id: string, api_key: string?): number
	local response = session.handle_primary_image_request({
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = "/",
			headers = {},
			query = if api_key then { api_key = { api_key } } else {},
			body_raw = "",
		},
		params = { imageId = item_ids.encode_jellyfin_item_id(id) },
	} :: any) :: any
	return response.status
end

local function album_tags(): { [string]: string? }
	local raw = { IncludeItemTypes = { "MusicAlbum" }, Recursive = { "true" } }
	local tags = {}
	for _, item in listing.list_items(options.for_user(options.parse(raw :: any), fixture.users.owner)) do
		tags[item.Name] = item.ImageTags and item.ImageTags.Primary
	end
	return tags
end

-- Listings don't look at cover files, so the gone cover is still advertised until an image
-- request finds its file missing; that request fails and forgets the cover.
local before = album_tags()
assert(before.Gone == "gone" and before.Kept == "kept", "both albums advertise their covers")
assert(image_status(ids.gone, fixture.api_keys.desk) == 404, "the gone cover's image is not found")
assert(image_status(ids.kept, fixture.api_keys.desk) == 200, "the kept cover still serves")

local after = album_tags()
assert(after.Gone == nil, `the gone cover is no longer advertised, got {after.Gone}`)
assert(after.Kept == "kept", "the kept cover is still advertised")
assert(image_status(ids.gone) == 404, "the anonymous route finds nothing either")
