local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

type Params = { [string]: string }

local ids = fixture.ids

local function hex(id: number): string
	return item_ids.encode_jellyfin_item_id(id)
end

-- `Name:Type` for each item a listing returns, then its total when it has one.
local function list(list_items: (options.ItemsQueryOptions) -> any, params: Params, parent: number?): string
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	if parent then
		raw.ParentId = { hex(parent) }
	end
	local items, total = list_items(options.for_user(options.parse(raw :: any), fixture.users.admin))
	local names = {}
	for _, item in items do
		table.insert(names, `{item.Name}:{item.Type}`)
	end
	local listed = table.concat(names, ",")
	return if total then `{listed} ({total})` else listed
end

local function items(params: Params, parent: number?): string
	return list(listing.list_items, params, parent)
end

local function check(actual: string, expected: string, why: string?)
	assert(actual == expected, `{why or "listing"}: expected "{expected}", got "{actual}"`)
end

local function with(base: Params, extra: Params): Params
	local merged = table.clone(base)
	for key, value in extra do
		merged[key] = value
	end
	return merged
end

-- A playlist's tracks list under its ParentId.
do
	local in_order = "V1:Audio,S2:Audio,S1:Audio (3)"
	check(items({ IncludeItemTypes = "Audio", Recursive = "false", Limit = "400" }, ids.mix), in_order)
	check(items({}, ids.mix), in_order, "each track once, where it first appears")
	check(items({ StartIndex = "1", Limit = "1" }, ids.mix), "S2:Audio (3)")
	check(items({ IncludeItemTypes = "MusicAlbum" }, ids.mix), " (0)")
	check(
		items({ SortBy = "SortName", StartIndex = "1", Limit = "1" }, ids.mix),
		"V1:Audio (3)",
		"SortBy orders a playlist's tracks before they page"
	)
	for _, untranslatable in { "Default", "Album", "CommunityRating,IsFavoriteOrLiked" } do
		check(
			items({ SortBy = untranslatable }, ids.mix),
			in_order,
			`keys without a Lyra meaning keep playlist order: {untranslatable}`
		)
	end
	check(items({ Years = "2005" }, ids.mix), "V1:Audio (1)", "the listing's filters apply to a playlist's tracks")
	check(items({ Filters = "IsFavorite" }, ids.mix), "S2:Audio (1)")
end

-- Named ids keep request order unless a sort is named.
do
	local tracks = `{hex(ids.s2)},{hex(ids.s1)}`
	check(items({ Ids = tracks }), "S2:Audio,S1:Audio (2)", "request order without SortBy")
	check(
		items({ Ids = tracks, SortBy = "Default" }),
		"S1:Audio,S2:Audio (2)",
		"a SortBy without a Lyra meaning falls back to SortName"
	)
	check(items({ Ids = tracks, AlbumIds = hex(ids.solo), SortBy = "Album" }), "S1:Audio,S2:Audio (2)")
	check(
		items({ Ids = `{hex(ids.mix)},{hex(ids.liked)}`, IncludeItemTypes = "Playlist", SortBy = "SortName" }),
		"Liked:Playlist,Mix:Playlist (2)"
	)
end

-- A parent lists its own children when no type is named.
do
	check(items({}, ids.solo), "S1:Audio,S2:Audio (2)")
	check(items({}, ids.amy), " (0)", "an artist is only a name, with no children")
	check(items({}, ids.rock), " (0)", "a genre is only a name, with no children")
	check(items({ IncludeItemTypes = "MusicArtist" }, ids.solo), "Amy:MusicArtist (1)", "a named type still lists")
	for _, parent in { ids.solo, ids.mix } do
		check(
			items({ ExcludeItemTypes = "Audio" }, parent),
			" (0)",
			"excluding a parent's only child type leaves nothing"
		)
	end
end

local function playlists(filter: Params): string
	return items(with({ IncludeItemTypes = "Playlist", Recursive = "true" }, filter))
end

-- Playlists filter by favourite.
do
	check(playlists({}), "Liked:Playlist,Mix:Playlist,Played:Playlist,Shared:Playlist (4)")
	check(playlists({ Filters = "IsFavorite" }), "Liked:Playlist (1)")
	check(playlists({ IsFavorite = "false" }), "Mix:Playlist,Played:Playlist,Shared:Playlist (3)")
	check(
		playlists({ Filters = "IsFavorite", IsFavorite = "false" }),
		"Liked:Playlist (1)",
		"the IsFavorite filter overrides the IsFavorite parameter"
	)
	check(playlists({ Filters = "IsFavoriteOrLikes" }), "Liked:Playlist (1)")
	check(
		playlists({ Filters = "IsFavoriteOrLikes", IsFavorite = "false" }),
		" (0)",
		"a favourite or liked playlist can't also be no favourite"
	)
end

-- Listings filter by played state.
do
	-- A playlist is played once none of its tracks is unplayed, so an empty one is played.
	local played = "Liked:Playlist,Played:Playlist,Shared:Playlist (3)"
	check(playlists({ Filters = "IsPlayed" }), played)
	check(playlists({ IsPlayed = "true" }), played)
	check(playlists({ Filters = "IsUnplayed" }), "Mix:Playlist (1)")
	check(
		playlists({ Filters = "IsPlayed", IsPlayed = "false" }),
		played,
		"the IsPlayed filter overrides the IsPlayed parameter"
	)
	check(playlists({ Filters = "IsPlayed,IsUnplayed" }), " (0)", "played and unplayed together match nothing")
	check(items({ Ids = hex(ids.shared), IncludeItemTypes = "Playlist", Filters = "IsUnplayed" }), " (0)")

	local function albums(filter: string): string
		return items({ IncludeItemTypes = "MusicAlbum", Recursive = "true", Filters = filter })
	end
	check(albums("IsPlayed"), "Solo:MusicAlbum (1)", "an album is played once every track is")
	check(albums("IsUnplayed"), "Various:MusicAlbum (1)")

	check(items({ Filters = "IsPlayed", IsPlayed = "false" }, ids.mix), "S2:Audio,S1:Audio (2)")
	check(items({ Filters = "IsUnplayed", IsPlayed = "true" }, ids.mix), "V1:Audio (1)")
end

-- Tracks sort by their album artist.
do
	local function sorted(sort_by: string): string
		return items({ IncludeItemTypes = "Audio", Recursive = "true", SortBy = sort_by })
	end
	check(
		sorted("AlbumArtist,Album,SortName"),
		"S1:Audio,S2:Audio,V1:Audio (3)",
		"the compilation track sorts under Various Artists"
	)
	check(sorted("Artist,Album,SortName"), "V1:Audio,S1:Audio,S2:Audio (3)")
end

-- The latest audio groups into albums.
do
	local function latest(params: Params): string
		return list(listing.list_latest_items, with({ IncludeItemTypes = "Audio" }, params))
	end
	check(
		latest({}),
		"Solo:MusicAlbum,Various:MusicAlbum",
		"albums order by the date they were added, not their newest track"
	)
	check(latest({ Limit = "1" }), "Solo:MusicAlbum", "the limit counts albums")
	check(latest({ GroupItems = "false" }), "V1:Audio,S2:Audio,S1:Audio")
end
