local enrichment = require("../../protocol/items/enrichment")
local options = require("../../protocol/items/options")
local query = require("../../protocol/items/query")

local function parse(params: { [string]: string }): options.ItemsQueryOptions
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.parse(raw :: any)
end

local function keys(entity: query.Entity, params: { [string]: string }, single_release: boolean?): string
	local terms = query.sort(entity, parse(params), single_release).sort
	if terms == nil then
		return "request order"
	end
	local values: { string } = {}
	for _, term in terms do
		table.insert(values, `{term.key} {term.order}`)
	end
	return table.concat(values, ",")
end

local root: query.Scope = { db_id = nil, entity_type = nil }

-- A track's SortName is disc, track and title.
assert(keys("track", {}) == "disc ascending,track ascending,sort_name ascending", keys("track", {}))
assert(keys("release", {}) == "sort_name ascending")
assert(keys("genre", { SortBy = "SortName" }) == "name ascending")

-- Each key takes the order at its index; keys past the orders take the first.
local paired = keys("track", { SortBy = "PlayCount,Name,DateCreated", SortOrder = "Descending,Ascending" })
assert(paired == "listen_count descending,name ascending,date_created descending", paired)

-- Keys without a Lyra meaning are dropped; repeated keys keep their first order.
local dropped = keys("release", { SortBy = "CommunityRating,Budget,Album,Name", SortOrder = "Descending" })
assert(dropped == "name descending", dropped)

-- Only one release's tracks have a release title to sort by.
assert(keys("track", { SortBy = "Album" }) == "")
assert(keys("track", { SortBy = "Album" }, true) == "release_title ascending")

assert(keys("track", { SortBy = "PremiereDate" }) == "year ascending")
-- A track's album artist is its release's, so a compilation's tracks sort together.
assert(keys("track", { SortBy = "AlbumArtist" }) == "release_artist_name ascending")
assert(keys("release", { SortBy = "PremiereDate,Runtime" }) == "release_date ascending,total_duration ascending")
assert(keys("artist", { SortBy = "AlbumArtist" }) == "")

-- Defaults: request order with Ids, newest first for an artist's albums, relevance when searching.
assert(keys("track", { Ids = "00000000000000000000000000000000" }) == "request order")
local artist_albums = keys("release", { AlbumArtistIds = "0123456789abcdef0123456789abcdef" })
assert(artist_albums == "year descending,sort_name ascending", artist_albums)
assert(keys("artist", { SearchTerm = "amy" }) == "relevance ascending")
assert(keys("artist", { SearchTerm = "amy", SortBy = "Name" }) == "name ascending")

local random = query.sort("track", parse({ SortBy = "Random" }))
assert(random.seed ~= nil)
assert(query.sort("track", parse({})).seed == nil)

-- Filters without a Lyra meaning match nothing or nothing more.
assert(query.tracks(parse({ Filters = "Likes" }), root) == nil)
assert(query.tracks(parse({ Filters = "IsResumable" }), root) == nil)
assert(query.tracks(parse({ Filters = "Dislikes" }), root) ~= nil)
assert(query.releases(parse({ MinCommunityRating = "5" }), root) == nil)
-- As upstream, `Filters=IsFavorite` overrides `IsFavorite`; `IsFavoriteOrLikes` must also hold.
local overridden = query.releases(parse({ Filters = "IsFavorite", IsFavorite = "false" }), root)
assert(overridden ~= nil and overridden.favorite == true)
assert(query.releases(parse({ Filters = "IsFavoriteOrLikes", IsFavorite = "false" }), root) == nil)
assert(query.genres(parse({ IsFavorite = "true" }), root) == nil)

local unfavorite = query.releases(parse({ IsFavorite = "false", Filters = "IsUnplayed" }), root)
assert(unfavorite ~= nil and unfavorite.favorite == false and unfavorite.listened == false)
local favorite = query.tracks(parse({ Filters = "IsFavoriteOrLikes,IsPlayed" }), root)
assert(favorite ~= nil and favorite.favorite == true and favorite.listened == true)

-- Playlists can only be filtered by favourite, which `Filters` and `IsFavorite` ask for together.
local liked, liked_ok = query.favorite(parse({ Filters = "IsFavorite" }))
assert(liked == true and liked_ok)
assert(select(2, query.favorite(parse({ Filters = "IsFavoriteOrLikes", IsFavorite = "false" }))) == false)
assert(not query.filters_beyond_user_state(parse({ Filters = "IsFavorite,IsFavoriteOrLikes,IsUnplayed" })))
assert(query.filters_beyond_user_state(parse({ Filters = "IsFavorite,Likes" })))

-- As upstream, `Filters=IsPlayed` or `IsUnplayed` overrides `IsPlayed`; naming both matches nothing.
assert(query.played(parse({ Filters = "IsUnplayed", IsPlayed = "true" })) == false)
assert(query.played(parse({ IsPlayed = "true" })) == true)
assert(select(2, query.played(parse({ Filters = "IsPlayed,IsUnplayed" }))) == false)
assert(query.tracks(parse({ Filters = "IsPlayed,IsUnplayed" }), root) == nil)

local ranged = query.artists(
	parse({ NameStartsWith = "b", NameStartsWithOrGreater = "a", NameLessThan = "c", SearchTerm = "x" }),
	root,
	"release"
)
assert(ranged ~= nil)
assert(ranged.sort_name_prefix == "b" and ranged.sort_name_at_least == "a" and ranged.sort_name_below == "c")
assert(ranged.search == "x" and ranged.credit_role == "release")

-- Ids that name no item match nothing rather than everything.
assert(query.tracks(parse({ ArtistIds = "0123456789abcdef0123456789abcdef" }), root) == nil)
assert(query.tracks(parse({ GenreIds = "0123456789abcdef0123456789abcdef" }), root) == nil)

local paged = query.tracks(parse({ StartIndex = "20", Limit = "10", Years = "1999,2001" }), root)
assert(paged ~= nil and paged.offset == 20 and paged.limit == 10)
assert(paged.years ~= nil and #paged.years == 2)

-- A parent that isn't a library, release, artist or genre has no tracks.
assert(query.tracks(parse({}), { db_id = 7, entity_type = "Playlist" }) == nil)
local in_library = query.tracks(parse({}), { db_id = 7, entity_type = "Library" })
assert(in_library ~= nil and in_library.library_id == 7)
local on_release = query.tracks(parse({}), { db_id = 7, entity_type = "Release" })
assert(on_release ~= nil and on_release.release_ids ~= nil and on_release.release_ids[1] == 7)
assert(keys("track", { SortBy = "Album" }, true) == "release_title ascending")
local by_artist = query.releases(parse({}), { db_id = 7, entity_type = "Artist" })
assert(by_artist ~= nil and by_artist.credit_role == "release" and by_artist.artist_ids ~= nil)

-- IncludeItemTypes wins over ExcludeItemTypes.
assert(options.wants_item_type(parse({ IncludeItemTypes = "Audio", ExcludeItemTypes = "Audio" }), "Audio"))
assert(not options.wants_item_type(parse({ ExcludeItemTypes = "Audio" }), "Audio"))
assert(not options.wants_item_type(parse({ MediaTypes = "Audio" }), "MusicAlbum"))

-- Upstream rejects `Filters` naming both sides of a pair.
assert(options.has_conflicting_filters(parse({ Filters = "IsPlayed,IsUnplayed" })))
assert(options.has_conflicting_filters(parse({ Filters = "isnotfolder,IsFolder" })))
assert(options.has_conflicting_filters(parse({ Filters = "Likes,Dislikes" })))
assert(not options.has_conflicting_filters(parse({ Filters = "IsFavorite,IsPlayed" })))

assert(enrichment.release_year("2004-05-06") == 2004)
assert(enrichment.release_date_to_premiere_date("2004") == "2004-01-01T00:00:00.0000000Z")
assert(enrichment.sum_runtime_ticks({}) == 0)
