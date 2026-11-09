local fixture = ...
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local playlist_access = require("../../protocol/playlist_access")

type Params = { [string]: string }

local ids = fixture.ids

local function hex(id: string): string
	return item_ids.encode(id)
end

-- `Name:Type` for each item a listing returns, then its total when it has one.
local function list(list_items: (options.ItemsQueryOptions) -> any, params: Params, parent: string?): string
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

local function items(params: Params, parent: string?): string
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
	for _, untranslatable in { "Default", "CommunityRating,IsFavoriteOrLiked" } do
		check(
			items({ SortBy = untranslatable }, ids.mix),
			in_order,
			`keys without a Lyra meaning keep playlist order: {untranslatable}`
		)
	end
	check(items({ SortBy = "Album" }, ids.mix), "S1:Audio,S2:Audio,V1:Audio (3)", "a playlist's tracks sort by album")
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

-- Under a parent, playlists list when they are the only type named, and only those with a track
-- under the parent.
do
	local function under(parent: string, filter: Params?): string
		return items(with({ IncludeItemTypes = "Playlist", Recursive = "true" }, filter or {}), parent)
	end
	check(under(ids.compilation), "Mix:Playlist (1)")
	check(under(ids.solo), "Mix:Playlist,Played:Playlist,Shared:Playlist (3)")
	check(under(ids.aaron), "Mix:Playlist (1)", "a track's artist holds the playlists it is on")
	check(under(ids.various_artists), "Mix:Playlist (1)", "so does its album's artist")
	check(under(ids.rock), " (0)", "a genre holds no tracks")
	check(under(ids.compilations), "Mix:Playlist (1)", "a library holds the playlists its tracks are on")
	check(under(ids.albums), "Mix:Playlist,Played:Playlist,Shared:Playlist (3)")
	check(
		under(ids.solo, { SortOrder = "Descending", StartIndex = "1", Limit = "1", IsFavorite = "false" }),
		"Played:Playlist (3)",
		"the other filters, the sort and the page still apply"
	)
	check(under(ids.solo, { Filters = "IsUnplayed" }), "Mix:Playlist (1)")
	check(
		items({ IncludeItemTypes = "Playlist,Audio", Recursive = "true" }, ids.compilation),
		"V1:Audio (1)",
		"playlists don't list under a parent beside another type"
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

-- Tracks sort by their album artist and album.
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
	check(sorted("SortName"), "S1:Audio,V1:Audio,S2:Audio (3)")
	check(sorted("Album,SortName"), "S1:Audio,S2:Audio,V1:Audio (3)", "library-wide tracks sort by album title")
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

-- The playlists view lists the playlists the caller sees, whatever type is named, as upstream.
do
	constants.configure_root_item_id(string.rep("1", 32))
	local folder = constants.PLAYLISTS_FOLDER_ID
	local view = "Playlists:ManualPlaylistsFolder (1)"
	check(
		list(listing.list_views, {}),
		"Albums:CollectionFolder,Compilations:CollectionFolder,Playlists:ManualPlaylistsFolder (3)",
		"the view shows beside the libraries while the caller sees a playlist"
	)
	check(items({ IncludeItemTypes = "ManualPlaylistsFolder", ExcludeItemTypes = "CollectionFolder" }), view)
	check(items({ Ids = folder }), view)
	local all = "Liked:Playlist,Mix:Playlist,Played:Playlist,Shared:Playlist (4)"
	check(items({ ParentId = folder, SortBy = "SortName" }), all)
	check(items({ ParentId = folder, IncludeItemTypes = "Audio" }), all, "the view lists playlists only")
	check(items({ ParentId = folder, Filters = "IsFavorite" }), "Liked:Playlist (1)")
end

-- As upstream, an entry's PlaylistItemId is its track's id, which names every entry of the track.
do
	local mix = playlist_access.find(ids.mix) :: any
	local entries = fixture.entries.mix
	local tracks = listing.list_playlist_track_items(mix, options.for_user(options.parse(nil), fixture.users.admin))
	assert(#tracks == 4, `four entries, got {#tracks}`)
	for _, track in tracks do
		assert(track.PlaylistItemId == track.Id, `PlaylistItemId {track.PlaylistItemId} is the track id {track.Id}`)
	end
	local matched = playlist_access.entries_of_items(mix, { hex(ids.s2), "unknown", hex(ids.liked) })
	assert(
		#matched == 2 and matched[1] == entries[2] and matched[2] == entries[4],
		"a track names each of its entries, and ids that match none are ignored"
	)
	assert(#playlist_access.entries_of_items(mix, { hex(ids.v1) }) == 1)
end

-- As upstream, `EnableTotalRecordCount=false` counts only the page of a listing its database
-- answers: a recursive one, one by `Ids`, and one at the root or of a library's items by type. A
-- parent's own children and a search count every match; playlists alone and listings by name
-- count none.
do
	local function uncounted(params: Params): Params
		return with(params, { EnableTotalRecordCount = "false" })
	end
	local audio = { Recursive = "true", IncludeItemTypes = "Audio" }
	check(items(with(audio, { Limit = "1" })), "S1:Audio (3)", "counted by default")
	check(items(uncounted(with(audio, { Limit = "1" }))), "S1:Audio (1)", "a recursive page")
	check(items(uncounted(with(audio, { StartIndex = "2" }))), "S2:Audio (1)", "the last page")
	check(items(uncounted(with(audio, { Limit = "0" }))), " (0)", "an empty page")
	check(
		items(uncounted({ Recursive = "true", IncludeItemTypes = "MusicAlbum,Audio", Limit = "1" })),
		"Solo:MusicAlbum (1)",
		"a page across types"
	)
	check(items(uncounted({ Ids = `{hex(ids.s2)},{hex(ids.s1)}`, Limit = "1" })), "S2:Audio (1)", "a page by Ids")
	check(items(uncounted({ IncludeItemTypes = "Audio", Limit = "1" })), "S1:Audio (1)", "a page at the root")
	check(
		items(uncounted({ IncludeItemTypes = "Audio", Limit = "1" }), fixture.libraries.albums),
		"S1:Audio (1)",
		"a page of a library's tracks"
	)
	check(items(uncounted({ Limit = "1" }), ids.solo), "S1:Audio (2)", "an album's own tracks")
	check(items(uncounted({ StartIndex = "1", Limit = "1" }), ids.mix), "S2:Audio (3)", "a playlist's own tracks")
	check(items(uncounted(with(audio, { SearchTerm = "S", Limit = "1" }))), "S1:Audio (2)", "a search")
	check(
		items(uncounted({ Ids = `{hex(ids.s1)},{hex(ids.s2)}`, SearchTerm = "S", Limit = "1" })),
		"S1:Audio (2)",
		"a search by Ids"
	)
	check(
		items(uncounted({ IncludeItemTypes = "Audio", Recursive = "false", Limit = "1" }), fixture.libraries.albums),
		"S1:Audio (2)",
		"a library's own items by type"
	)

	check(
		items(uncounted({ IncludeItemTypes = "Playlist", Recursive = "true", SortBy = "SortName", Limit = "1" })),
		"Liked:Playlist (0)",
		"playlists"
	)
	check(
		items(uncounted({ ParentId = constants.PLAYLISTS_FOLDER_ID, SortBy = "SortName", Limit = "1" })),
		"Liked:Playlist (0)",
		"the playlists view"
	)
	check(
		items(uncounted({ IncludeItemTypes = "Playlist", Ids = `{hex(ids.mix)},{hex(ids.liked)}` })),
		"Mix:Playlist,Liked:Playlist (2)",
		"playlists by Ids"
	)

	local function by_name(list_items: (options.ItemsQueryOptions) -> any, params: Params): string
		return list(list_items, params)
	end
	local function artists(params: Params): string
		return by_name(function(listed)
			return listing.list_artist_items(listed, nil)
		end, params)
	end
	check(artists({ Limit = "1" }), "Aaron:MusicArtist (3)", "artists counted by default")
	check(artists(uncounted({ Limit = "1" })), "Aaron:MusicArtist (0)", "artists uncounted")
	check(by_name(listing.list_genre_items, uncounted({})), "Rock:MusicGenre (0)", "genres uncounted")
end
