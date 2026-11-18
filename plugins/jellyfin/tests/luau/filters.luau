local fixture = ...
local genres = require("@lyra/genres")
local tags = require("@lyra/tags")
local tracks = require("@lyra/tracks")

local filters = require("../../protocol/items/filters")
local item_ids = require("../../protocol/items/identifiers")
local lookups = require("../../protocol/items/lookups")
local options = require("../../protocol/items/options")
local query = require("../../protocol/items/query")

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

check(values({}), "Blues,jazz,Rock|Band,Favourite,Vinyl|1999,2001,2009", "every item type")
check(values({ IncludeItemTypes = "MusicAlbum" }), "Blues,jazz,Rock|Favourite|1999", "albums carry their date's year")
-- S2 has no year of its own, so it lends none, as filtering by its album's year wouldn't find it.
check(values({ IncludeItemTypes = "Audio" }), "Blues,jazz,Rock|Vinyl,favourite|2001,2009", "tags order ordinally")
check(values({ IncludeItemTypes = "MusicArtist" }), "|Band|", "artists carry tags only")
check(
	values({ IncludeItemTypes = "Audio", ParentId = item_ids.encode(ids.solo) }),
	"jazz,Rock|favourite|2001",
	"under an album"
)
check(values({ ParentId = item_ids.encode(ids.s1) }), "none", "a track is no folder")

-- `Tags` matches the caller's tags ignoring case, as the values group them.
check(values({ IncludeItemTypes = "Audio", Tags = "FAVOURITE" }), "jazz,Rock|favourite|", "a tag of any case")
check(
	values({ IncludeItemTypes = "Audio", Tags = "favourite|vinyl" }),
	"Blues,jazz,Rock|Vinyl,favourite|2009",
	"several tags, pipe-delimited"
)
check(values({ IncludeItemTypes = "MusicAlbum", Tags = "Nope" }), "||", "a tag the caller lacks")
do
	local listed = query.tracks(
		options.for_user(options.parse({ Tags = { "Favourite" } } :: any), fixture.users.admin),
		lookups.scope(nil)
	)
	assert(listed ~= nil, "a tag of the caller's narrows the listing")
	local titles = {}
	for _, track in tracks.query(listed).items do
		table.insert(titles, track.track_title)
	end
	check(table.concat(titles, ","), "S2", "the listing keeps the tagged track")
end

-- An artist's genres are those of the albums it is credited on.
do
	local function genre_names(params: { [string]: { string } }): string
		local listed = query.genres(
			options.for_user(options.parse(params :: any), fixture.users.admin),
			lookups.scope(params.ParentId and params.ParentId[1])
		)
		if not listed then
			return "none"
		end
		local names = {}
		for _, genre in genres.query(listed).items do
			table.insert(names, genre.name)
		end
		return table.concat(names, ",")
	end
	check(genre_names({ ParentId = { item_ids.encode(ids.amy) :: string } }), "jazz,Rock", "under an artist")
	check(genre_names({ ParentId = { item_ids.encode(ids.ben) :: string } }), "Blues", "under another artist")
	check(genre_names({ AlbumIds = { item_ids.encode(ids.live) :: string } }), "Blues", "of an album")
	check(genre_names({ IsFavorite = { "true" } }), "none", "genres are never favourites")
	check(genre_names({ Tags = { "Favourite" } }), "none", "genres carry no tags")
end
check(values({ IncludeItemTypes = "Playlist" }), "||", "no music item type")

-- The legacy route answers names; the newer one gives genres their ids, for one music type only.
do
	local function collect(params: { [string]: { string } })
		return filters.collect(options.for_user(options.parse(params :: any), fixture.users.admin))
	end
	local legacy = filters.legacy_json(collect({})) :: any
	check(table.concat(legacy.Genres, ","), "Blues,Rock,jazz", "legacy genres order ordinally")
	check(table.concat(legacy.Years, ","), "1999,2001,2009", "legacy years")
	assert(#legacy.OfficialRatings == 0)

	local albums = filters.to_json(collect({ IncludeItemTypes = { "MusicAlbum" } }), { "MusicAlbum" }) :: any
	check(albums.Genres[1].Name, "Blues", "the first genre")
	check(albums.Genres[1].Id, item_ids.encode(ids.blues), "a genre's id")
	local mixed = filters.to_json(collect({}), {}) :: any
	assert(#mixed.Genres == 0, "no music genres without a single music type")
	check(table.concat(mixed.Tags, ","), "Band,Favourite,Vinyl", "tags")
	assert(#mixed.AudioLanguages == 0 and #mixed.SubtitleLanguages == 0)
end
