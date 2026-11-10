local fixture = ...

local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local playback = require("../../protocol/items/playback")

constants.configure_root_item_id(string.rep("1", 32))

local function status(raw_id: string): number
	local context: any = { params = { id = raw_id } }
	local id, track, response = playback.resolve_playback_info_item(context, fixture.users.listener)
	if response then
		return (response :: any).status
	end
	assert(id ~= nil and track ~= nil, "a resolved item names its track")
	return 200
end

local function of(key: string): string
	return item_ids.encode(fixture.ids[key]) :: string
end

assert(status(of("song")) == 200, "a track plays")

-- Upstream answers 400 for a visible item without media sources, synthetic folders included.
for _, key in { "album", "band", "rock", "mine" } do
	assert(status(of(key)) == 400, `{key} has no media sources`)
end
local library = item_ids.encode(fixture.libraries.music) :: string
assert(status(library) == 400, "a library has no media sources")
assert(status(constants.ROOT_ITEM_ID) == 400, "the root has no media sources")
assert(status(constants.PLAYLISTS_FOLDER_ID) == 400, "the playlists folder has no media sources")

-- What the caller can't see stays not found.
for _, key in { "secret_album", "secret_song", "private" } do
	assert(status(of(key)) == 404, `{key} is hidden`)
end
assert(status("00000000000000000000000000000abc") == 404, "an unknown id")
assert(status("not-an-id") == 404, "a malformed id")
