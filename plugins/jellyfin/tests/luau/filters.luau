local fixture = ...
local tags = require("@lyra/tags")

local filters = require("../../protocol/items/filters")
local item_ids = require("../../protocol/items/identifiers")
local options = require("../../protocol/items/options")

local ids = fixture.ids

tags.add(ids.solo, "Favourite", "#ff0000")
tags.add(ids.s2, "favourite", "#ff0000")
tags.add(ids.l1, "Vinyl", "#00ff00")
tags.add(ids.ben, "Band", "#0000ff")

-- `genres|tags|years` for the filters of a listing.
local function values(params: { [string]: string }): string
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	local found = filters.collect(options.for_user(options.parse(raw :: any), fixture.users.admin))
	if not found then
		return "none"
	end
	local genre_names = {}
	for _, genre in found.genres do
		table.insert(genre_names, genre.name)
	end
	local years = {}
	for _, year in found.years do
		table.insert(years, tostring(year))
	end
	return `{table.concat(genre_names, ",")}|{table.concat(found.tags, ",")}|{table.concat(years, ",")}`
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

check(values({}), "Blues,jazz,Rock|Band,Favourite,Vinyl|2001,2009", "every item type")
check(values({ IncludeItemTypes = "MusicAlbum" }), "Blues,jazz,Rock|Favourite|", "albums carry no year here")
check(values({ IncludeItemTypes = "Audio" }), "Blues,jazz,Rock|Vinyl,favourite|2001,2009", "tags order ordinally")
check(values({ IncludeItemTypes = "MusicArtist" }), "|Band|", "artists carry tags only")
check(
	values({ IncludeItemTypes = "Audio", ParentId = item_ids.encode_jellyfin_item_id(ids.solo) }),
	"jazz,Rock|favourite|2001",
	"under an album"
)
check(values({ ParentId = item_ids.encode_jellyfin_item_id(ids.s1) }), "none", "a track is no folder")
check(values({ IncludeItemTypes = "Playlist" }), "||", "no music item type")

-- The legacy route answers names; the newer one gives genres their ids, for one music type only.
do
	local function collect(params: { [string]: { string } })
		return filters.collect(options.for_user(options.parse(params :: any), fixture.users.admin))
	end
	local legacy = filters.legacy_json(collect({})) :: any
	check(table.concat(legacy.Genres, ","), "Blues,Rock,jazz", "legacy genres order ordinally")
	check(table.concat(legacy.Years, ","), "2001,2009", "legacy years")
	assert(#legacy.OfficialRatings == 0)

	local albums = filters.to_json(collect({ IncludeItemTypes = { "MusicAlbum" } }), { "MusicAlbum" }) :: any
	check(albums.Genres[1].Name, "Blues", "the first genre")
	check(albums.Genres[1].Id, item_ids.encode_jellyfin_item_id(ids.blues), "a genre's id")
	local mixed = filters.to_json(collect({}), {}) :: any
	assert(#mixed.Genres == 0, "no music genres without a single music type")
	check(table.concat(mixed.Tags, ","), "Band,Favourite,Vinyl", "tags")
	assert(#mixed.AudioLanguages == 0 and #mixed.SubtitleLanguages == 0)
end
