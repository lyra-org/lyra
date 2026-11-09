local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local playlist_access = require("../../protocol/playlist_access")
local session = require("../../protocol/session")

-- `CanDelete` says what the item delete route would answer: a playlist deletes for its owner or
-- an admin, and nothing else deletes. As upstream, an item carries it when `Fields` asks, and a
-- single-item response always.

local ids = fixture.ids
local listener = fixture.users.listener

local function hex(id: string): string
	return item_ids.encode(id) :: string
end

-- `Name=CanDelete` for each item of a listing.
local function listed(
	list: (options.ItemsQueryOptions) -> any,
	params: { [string]: string },
	is_admin: boolean?
): string
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	local values = {}
	for _, item in list(options.for_user(options.parse(raw :: any), listener, is_admin)) do
		table.insert(values, `{item.Name}={tostring(item.CanDelete)}`)
	end
	return table.concat(values, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

local playlists = { IncludeItemTypes = "Playlist", Recursive = "true" }
check(listed(listing.list_items, playlists), "Mine=nil,Shared=nil", "absent unless Fields asks")
playlists.Fields = "CanDelete"
check(listed(listing.list_items, playlists), "Mine=true,Shared=false", "the owner deletes")
check(listed(listing.list_items, playlists, true), "Mine=true,Shared=true", "an admin deletes any it sees")

local catalog_items = { IncludeItemTypes = "Audio,MusicAlbum,MusicArtist", Recursive = "true", Fields = "candelete" }
check(
	listed(listing.list_items, catalog_items, true),
	"Album=false,Amy=false,Song=false",
	"no catalog item deletes, for an admin either"
)
check(listed(listing.list_genre_items, { Fields = "CanDelete" }, true), "Rock=false", "no genre deletes")
check(listed(listing.list_genre_items, {}), "Rock=nil", "a genre listing leaves it out unless asked")
check(
	listed(listing.list_items, { Ids = hex(ids.rock), Fields = "CanDelete" }),
	"Rock=false",
	"a genre named by id carries it too"
)

-- A single-item response carries it whatever the request.
local function single(key: string, is_admin: boolean): boolean?
	local item = listing.find_item_by_id(ids[key], listener, { is_admin = is_admin })
	assert(item, `{key} is found`)
	return item.CanDelete
end
assert(single("mine", false) == true, "the owner's own playlist")
assert(single("shared", false) == false, "another user's public playlist")
assert(single("shared", true) == true, "an admin's view of it")
for _, key in { "song", "album", "amy", "rock" } do
	assert(single(key, true) == false, `{key} never deletes`)
end
local lookup = listing.find_item_by_id(ids.mine, listener)
assert(lookup and lookup.CanDelete == nil, "an internal lookup leaves it out")

-- The route agrees with the field.
local function delete(key: string): number
	return session.handle_delete_item_route({
		plugin_id = "jellyfin",
		request = {
			method = "DELETE",
			path = "/Items",
			headers = { authorization = `MediaBrowser Token="{fixture.api_keys.phone}"` },
			query = {},
			body_raw = "",
			json = nil,
		},
		params = { id = hex(ids[key]) },
	} :: any).status
end
assert(delete("shared") == 401, "the route refuses what the field denies")
assert(delete("mine") == 204, "and deletes what it allows")
assert(playlist_access.find(ids.mine) == nil, "the playlist is gone")
