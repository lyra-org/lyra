local fixture = ...
local instant_mix = require("../../protocol/items/instant_mix")
local options = require("../../protocol/items/options")

local ids = fixture.ids

local function query(params: { [string]: string }): options.ItemsQueryOptions
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.listener)
end

-- `Name,...` of a mix, its total, and whether its seed was found.
local function names(items: { any }, total: number, found: boolean): (string, number, boolean)
	local listed = {}
	for _, item in items do
		table.insert(listed, item.Name)
	end
	return table.concat(listed, ","), total, found
end

local function sorted(list: string): string
	local parts = string.split(list, ",")
	table.sort(parts)
	return table.concat(parts, ",")
end

local function mix_of(item_id: number?, params: { [string]: string }?): (string, number, boolean)
	local items, total, _, found = instant_mix.list_instant_mix_audio(item_id, query(params or {}))
	return names(items, total, found)
end

-- A song seed leads its mix; other seeds mix what the listener can see.
local listed, total, found = mix_of(ids.s2)
assert(found and total == 3 and string.sub(listed, 1, 2) == "S2", `song mix: {listed}`)
assert(sorted(listed) == "S1,S2,S3", `song mix: {listed}`)
for _, seed in { ids.solo, ids.amy, ids.mix, ids.rock } do
	listed, total, found = mix_of(seed)
	assert(found and total == 3 and sorted(listed) == "S1,S2,S3", `mix of {seed}: {listed}`)
end

-- Paging slices the mix and keeps its total.
listed, total = mix_of(ids.s2, { StartIndex = "0", Limit = "1" })
assert(listed == "S2" and total == 1, `paged: {listed} ({total})`)

-- A hidden or unknown seed is not found; a library lists nothing.
assert(not select(3, mix_of(ids.h1)), "hidden seed")
assert(not select(3, mix_of(999999999)), "unknown seed")
listed, total, found = mix_of(ids.music)
assert(found and listed == "" and total == 0, `library: {listed}`)

-- Genres mix by name.
local items, genre_total, _, genre_found = instant_mix.list_instant_mix_audio_from_genre_name("Rock", query({}))
listed = names(items, genre_total, genre_found)
assert(genre_found and sorted(listed) == "S1,S2,S3", `genre by name: {listed}`)
assert(not select(4, instant_mix.list_instant_mix_audio_from_genre_name("Jazz", query({}))), "unknown genre")
