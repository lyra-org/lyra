local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- An album's Overview is its release description, sent when the client asks for it.

local ids = fixture.ids

local function overview(release_id: string, fields: string?): string?
	local raw: { [string]: { string } } = { Ids = { item_ids.encode_jellyfin_item_id(release_id) } }
	if fields then
		raw.Fields = { fields }
	end
	local listed = listing.list_items(options.for_user(options.parse(raw :: any), fixture.users.admin))
	assert(#listed == 1, `expected one item, got {#listed}`)
	return listed[1].Overview
end

assert(overview(ids.solo, "Overview") == "Solo's story", "the trimmed description")
assert(overview(ids.solo) == nil, "omitted unless requested")
assert(overview(ids.plain, "Overview") == nil, "absent without a description")

local album = listing.find_item_by_id(ids.solo, fixture.users.admin)
assert(album and album.Overview == "Solo's story", "the single-item lookup includes it")
