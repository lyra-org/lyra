local fixture = ...
local instant_mix = require("../../protocol/items/instant_mix")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local playback = require("../../protocol/items/playback")
local playlist_access = require("../../protocol/playlist_access")

type Params = { [string]: string }

local ids = fixture.ids

local function hex(id: string): string
	return item_ids.encode(id)
end

local function parsed(params: Params): options.ItemsQueryOptions
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.admin)
end

-- `Name:Type` for each item a listing returns, then its total.
local function items(params: Params): string
	local found, total = listing.list_items(parsed(params))
	local names = {}
	for _, item in found do
		table.insert(names, `{item.Name}:{item.Type}`)
	end
	return `{table.concat(names, ",")} ({total})`
end

local function check(actual: any, expected: any, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

-- Clients can't show tracks whose files are missing, so listings leave them out.
check(items({ IncludeItemTypes = "Audio", Recursive = "true" }), "Playing:Audio (1)", "tracks")
check(items({ IncludeItemTypes = "MusicAlbum", Recursive = "true" }), "Mixed:MusicAlbum (1)", "albums")
check(items({ ParentId = hex(ids.mixed) }), "Playing:Audio (1)", "an album's tracks")

-- An item read directly is not found, and can't play.
check(listing.find_item_by_id(ids.missing, fixture.users.admin), nil, "a missing track by id")
check(listing.find_item_by_id(ids.lost, fixture.users.admin), nil, "an album with nothing to play")
assert(listing.find_item_by_id(ids.playing, fixture.users.admin) ~= nil, "a playing track by id")
check(playback.resolve_track_for_playback(ids.missing), nil, "playing a missing track")
assert(playback.resolve_track_for_playback(ids.playing) ~= nil, "playing a track that plays")

-- A playlist lists only what can play.
local mine = playlist_access.find(ids.mine) :: any
local entries = listing.list_playlist_track_items(mine, parsed({}))
check(#entries, 1, "a playlist's entries")
check(entries[1].Name, "Playing", "the playlist entry shown")

-- An instant mix of a missing track doesn't lead with it.
local mixed = instant_mix.list_instant_mix_audio(ids.missing, parsed({}))
for _, item in mixed do
	assert(item.Id ~= hex(ids.missing), "an instant mix led with a missing seed")
end
