local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local session = require("../../protocol/session")

-- A genre advertises its pick without looking at the file. When the image request finds the
-- file gone, the genre moves straight on to its next release with a cover: the next listing
-- advertises that one, never nothing.

local function rock_tag(): string?
	for _, item in listing.list_genre_items(options.for_user(options.parse({} :: any), fixture.users.admin)) do
		if item.Name == "Rock" then
			return item.ImageTags and item.ImageTags.Primary
		end
	end
	return nil
end

local function image_status(): number
	local response = session.handle_primary_image_request({
		plugin_id = "jellyfin",
		request = { method = "GET", path = "/", headers = {}, query = {}, body_raw = "" },
		params = { imageId = item_ids.encode(fixture.ids.rock) },
	} :: any)
	return if response.kind == "cover" then 200 else response.status
end

-- The listens make First the instance's pick, then Second once First's image fails, then Kept,
-- the only release left with a file.
local advertised = {}
for _ = 1, 3 do
	local tag = rock_tag()
	assert(tag ~= nil, "Rock always advertises a cover")
	table.insert(advertised, tag)
	local status = image_status()
	if tag == "kept" then
		assert(status == 200, `the kept cover serves, got {status}`)
		break
	end
	assert(status == 404, `a deleted cover is not served, got {status}`)
end
local order = table.concat(advertised, ",")
assert(order == "first,second,kept", `Rock moves past each deleted cover in turn, got {order}`)
