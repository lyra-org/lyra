local fixture = ...
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local session = require("../../protocol/session")

-- Genres and playlists advertise the cover of one release in them, tagged by its hash with its
-- blurhash, and their image routes serve it, anonymously as image requests come.

local function list(list_items: (options.ItemsQueryOptions) -> any, params: { [string]: string }?)
	local raw: { [string]: { string } } = {}
	for key, value in params or {} do
		raw[key] = { value }
	end
	local listed = {}
	for _, item in list_items(options.for_user(options.parse(raw :: any), fixture.users.admin)) do
		listed[item.Name] = item
	end
	return listed
end

local function image_status(id: number): number
	local response = session.handle_primary_image_request({
		plugin_id = "jellyfin",
		request = { method = "GET", path = "/", headers = {}, query = {}, body_raw = "" },
		params = { imageId = item_ids.encode_jellyfin_item_id(id) },
	} :: any)
	return if response.kind == "cover" then 200 else response.status
end

local function genre_image_status(name: string): number
	local response = session.handle_genre_image_request({
		plugin_id = "jellyfin",
		request = { method = "GET", path = "/", headers = {}, query = {}, body_raw = "" },
		params = { name = name, imageType = "Primary" },
	} :: any)
	return if response.kind == "cover" then 200 else response.status
end

local function assert_covered(item: any, what: string)
	assert(item.ImageTags and item.ImageTags.Primary == "covered", `{what} is tagged by the cover's hash`)
	local blurhashes = item.ImageBlurHashes and item.ImageBlurHashes.Primary
	assert(blurhashes and blurhashes.covered == "L00000fQfQfQ", `{what} carries the cover's blurhash`)
end

local genres = list(listing.list_genre_items)
local rock, jazz = genres.Rock, genres.Jazz
assert(rock and jazz, "both genres list")
assert_covered(rock, "a genre with album art")
assert(image_status(fixture.ids.rock) == 200, "and its image is served")
assert(genre_image_status("Rock") == 200, "by name too")
assert(jazz.ImageTags == nil and jazz.ImageBlurHashes == nil, "a genre without album art has no image tag")
assert(image_status(fixture.ids.jazz) == 404, "and no image is served")
assert(genre_image_status("Jazz") == 404, "by name either")

local by_id = list(listing.list_items, {
	IncludeItemTypes = "MusicGenre",
	Ids = item_ids.encode_jellyfin_item_id(fixture.ids.rock) :: string,
})
assert_covered(by_id.Rock, "a genre looked up by id")

constants.configure_root_item_id(string.rep("1", 32))
local playlists = list(listing.list_items, { ParentId = constants.PLAYLISTS_FOLDER_ID })
assert(playlists.Mix, "the playlist lists")
assert_covered(playlists.Mix, "a playlist")
assert(image_status(fixture.ids.mix) == 200, "and its image is served")
