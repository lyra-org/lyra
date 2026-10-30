local fixture = ...
local constants = require("../../constants")
local covers = require("@lyra/covers")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local session = require("../../protocol/session")

-- A restricted listener whose genre's pick is a release they can't see. Image requests carry no
-- credentials, so the tag a listing advertises has to name the image the anonymous route serves.

local ids = fixture.ids
local hash_by_path = {}
for _, cover in covers.artwork({ ids.shown, ids.secret }) do
	hash_by_path[cover.path] = cover.hash
end

local function served_hash(id: number, api_key: string?): string?
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
	return if response.status == 200 then hash_by_path[response.path] else nil
end

local function list(list_items: (options.ItemsQueryOptions) -> any, params: { [string]: string })
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	local listed = {}
	for _, item in list_items(options.for_user(options.parse(raw :: any), fixture.users.listener)) do
		listed[item.Name] = item
	end
	return listed
end

-- The case under test: the listener's own pick falls back past the hidden winner.
local caller_pick = covers.get({ ids.rock }, { listens = "instance" })[ids.rock]
assert(caller_pick.release_id == ids.shown, "the listener's own pick is the release they see")
assert(covers.artwork({ ids.rock })[ids.rock].release_id == ids.secret, "the instance picks Secret")

local function assert_consistent(item: any, id: number, what: string)
	local tag = item.ImageTags and item.ImageTags.Primary
	assert(tag ~= nil, `{what} has an image tag`)
	assert(served_hash(id) == tag, `{what}: tag {tag}, anonymous image {served_hash(id)}`)
	assert(served_hash(id, fixture.api_keys.phone) == tag, `{what}: tag {tag}, signed-in image differs`)
	local blurhashes = item.ImageBlurHashes and item.ImageBlurHashes.Primary
	assert(blurhashes and blurhashes[tag], `{what} carries the tagged cover's blurhash`)
end

local genres = list(listing.list_genre_items, {})
assert(genres.Rock, "Rock lists for the listener")
assert(genres.Rock.ImageTags.Primary == "secret", "Rock is tagged with the instance's pick")
assert_consistent(genres.Rock, ids.rock, "Rock")

constants.configure_root_item_id(string.rep("1", 32))
local playlists = list(listing.list_items, { ParentId = constants.PLAYLISTS_FOLDER_ID })
assert(playlists.Mix, "the public playlist lists for the listener")
assert_consistent(playlists.Mix, ids.mix, "Mix")
