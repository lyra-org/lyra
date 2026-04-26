--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists_module = require("@lyra/artists")
local covers = require("@lyra/covers")
local entities = require("@lyra/entities")
local favorites = require("@lyra/favorites")
local genres = require("@lyra/genres")
local ids_module = require("@lyra/ids")
local images = require("@lyra/images")
local listens = require("@lyra/listens")
local playback_sources = require("@lyra/playback_sources")
local playlists = require("@lyra/playlists")
local releases = require("@lyra/releases")
local tags_module = require("@lyra/tags")
local track_sources = require("@lyra/track_sources")
local tracks = require("@lyra/tracks")

local constants = require("../../constants")
local libraries = require("@lyra/libraries")
local ITEM_ID_HEX_LENGTH = constants.ITEM_ID_HEX_LENGTH
local ITEM_ID_HEX_PATTERN = constants.ITEM_ID_HEX_PATTERN
local DEFAULT_ITEMS_FIELDS = constants.DEFAULT_ITEMS_FIELDS
local ROUTE_PREFIX = constants.ROUTE_PREFIX

local FAVORITE_ENTITY_KINDS = { "track", "release", "artist", "playlist" }

local base = require("./common")
local item_types = require("./types")
local serialization = require("./serialization")

-- Internal helpers table to reduce top-level local count.
local helpers = {}

function helpers.is_library_root(hex_id: string): boolean
	local nanoid = base.hex_to_nanoid(hex_id)
	if not nanoid then
		return false
	end
	local ok, element_type = pcall(entities.get_type, nanoid)
	return ok and element_type == "Library"
end

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type EntityProjectionInfo = entities.EntityProjectionInfo
type EntityQueryRequest = entities.EntityQueryRequest
type EntityQueryManyRequest = entities.EntityQueryManyRequest
type ReleaseProjectionInfo = entities.ReleaseProjectionInfo
type ArtistProjectionInfo = entities.ArtistProjectionInfo
type TrackProjectionInfo = entities.TrackProjectionInfo
type CreditedArtistProjectionInfo = entities.CreditedArtistProjectionInfo
type CreditType = entities.CreditType
type ArtistCreditSource = entities.ArtistCreditSource

local CreditType = entities.CreditType
local ArtistCreditSource = entities.ArtistCreditSource
type ApiQueryParams = api.ApiQueryParams
type ReleaseQueryOptions = releases.ReleaseQueryOptions
type ReleaseQueryResult = releases.ReleaseQueryResult
type ArtistQueryOptions = artists_module.ArtistQueryOptions
type ArtistQueryResult = artists_module.ArtistQueryResult
type TrackQueryOptions = tracks.TrackQueryOptions
type TrackQueryResult = tracks.TrackQueryResult
type Track = tracks.Track
type Release = releases.Release
type Artist = artists_module.Artist
type PlaylistInfo = playlists.PlaylistInfo
type PlaylistTrackLink = playlists.PlaylistTrackLink
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo
type TagInfo = tags_module.TagInfo
type TagInfoMap = { [number]: { TagInfo } }
type UserData = base.UserData
type ReleaseRecord = {
	id: index<Release, "id">,
	db_id: index<Release, "db_id">,
	release_title: index<Release, "release_title">,
	sort_title: index<Release, "sort_title">,
	release_date: index<Release, "release_date">,
	created_at: index<Release, "created_at">,
	ctime: index<Release, "ctime">,
}
type ArtistRecord = {
	id: index<Artist, "id">,
	db_id: index<Artist, "db_id">,
	artist_name: index<Artist, "artist_name">,
	sort_name: index<Artist, "sort_name">,
	created_at: index<Artist, "created_at">,
}
type TrackRecord = {
	id: index<Track, "id">,
	db_id: index<Track, "db_id">,
	track_title: index<Track, "track_title">,
	sort_title: index<Track, "sort_title">,
	disc: index<Track, "disc">,
	track: index<Track, "track">,
	duration_ms: index<Track, "duration_ms">,
	sample_rate_hz: index<Track, "sample_rate_hz">,
	channel_count: index<Track, "channel_count">,
	bit_depth: index<Track, "bit_depth">,
	bitrate_bps: index<Track, "bitrate_bps">,
	created_at: index<Track, "created_at">,
	ctime: index<Track, "ctime">,
}
type ReleaseLike = Release | ReleaseRecord
type ArtistLike = Artist | ArtistRecord
type TrackLike = Track | TrackRecord
type ItemRaw = ReleaseLike | ArtistLike | TrackLike | PlaylistInfo
type ItemKind = "release" | "artist" | "track" | "playlist"
type SortValueKind = "number" | "string"
type SortValue = number | string
type LibraryInfo = {
	db_id: number?,
	id: string,
	name: string,
	directory: string,
	language: string?,
	country: string?,
}
export type FavoriteLookup = { [number]: boolean }
export type ItemsQueryOptions = {
	include_item_types: { string },
	ids: { string },
	exclude_item_ids: { string },
	exclude_artist_ids: { string },
	primary_artist_ids: { string },
	contributing_artist_ids: { string },
	track_artist_ids: { string },
	release_artist_ids: { string },
	album_ids: { string },
	artist_ids: { string },
	genre_ids: { string },
	filters: { string },
	is_favorite: boolean?,
	parent_id: string?,
	start_index: number,
	limit: number?,
	recursive: boolean?,
	sort_by: { string },
	sort_order: string?,
	fields: { string },
	search_term: string?,
	enable_user_data: boolean?,
	enable_images: boolean?,
	include_media_info: boolean,
	include_people: boolean,
	group_items: boolean,
	user_id: number?,
	_favorite_id_lookup: FavoriteLookup?,
	_genre_infos_by_release_id: { [number]: { genres.GenreInfo } }?,
	_tag_infos_by_item_id: TagInfoMap?,
}
type CoverInfo = {
	path: string,
	mime_type: string,
	hash: string,
	blurhash: string?,
	release_id: string?,
}
type ArtistRelationMap = { [number]: { Artist }? }
type ReleaseRelationMap = { [number]: { Release }? }
type TrackRelationMap = { [number]: { Track }? }
type CreditRelationMap = { [number]: { CreditedArtistProjectionInfo }? }
type PlaybackSourceMap = { [number]: PlaybackSourceInfo? }
type ContainerMap = { [number]: string? }
type PlayCountLookup = { [number]: number }
type LastPlayedLookup = { [number]: number }
type ItemContext = {
	user_id: number?,
	favorite_lookup: FavoriteLookup,
	play_count_lookup: PlayCountLookup,
	last_played_lookup: LastPlayedLookup,
	include_user_data: boolean,
	include_images: boolean,
	include_people: boolean,
	related_artists_by_owner_id: ArtistRelationMap,
	credited_artists_by_owner_id: CreditRelationMap,
	release_tracks_by_release_id: TrackRelationMap,
	track_releases_by_track_id: ReleaseRelationMap,
	artist_tracks_by_artist_id: TrackRelationMap,
	playback_source_by_track_id: PlaybackSourceMap,
	container_by_track_id: ContainerMap,
	track_release_lookup_complete: boolean?,
	has_source_prefetch: boolean?,
	has_container_prefetch: boolean?,
	has_track_artists_prefetch: boolean?,
	has_track_releases_prefetch: boolean?,
	has_release_artists_prefetch: boolean?,
	has_release_tracks_prefetch: boolean?,
	has_artist_tracks_prefetch: boolean?,
	genre_infos_by_release_id: { [number]: { genres.GenreInfo } }?,
	tag_infos_by_item_id: TagInfoMap?,
	cover_info_by_release_id: { [number]: base.ResolvedCoverInfo }?,
	cover_prefetch_release_ids: { [number]: boolean }?,
	library_hex_id: string?,
	library_hex_id_by_entity_id: { [number]: string }?,
	playlist_track_ids_by_playlist_id: { [number]: { number } }?,
}
type RealizationSeedInfo = {
	track_ids: { number },
	release_ids: { number },
	artist_ids: { number },
	playlist_db_ids: { number },
	track_seen: { [number]: boolean },
	release_seen: { [number]: boolean },
	artist_seen: { [number]: boolean },
	has_track_stubs: boolean,
	has_release_stubs: boolean,
	has_artist_stubs: boolean,
}
type MediaStreamInfo = item_types.MediaStreamInfo
type MediaSourceInfo = item_types.MediaSourceInfo
type ArtistProjectionItem = item_types.ArtistProjectionItem
type ImageBlurHashes = item_types.ImageBlurHashes
type PersonItem = item_types.PersonItem
type PersonType = item_types.PersonType
export type ItemInfo = item_types.SerializableItemInfo & {
	_type: ItemKind?,
	_raw: ItemRaw?,
	_child_count: number?,
}

type ItemListResult = {
	items: { ItemInfo },
	total_count: number,
	start_index: number,
}
type ParentScopeInfo = {
	db_id: number?,
	entity_type: string?,
}
type ItemTypeSelection = {
	constrained: boolean,
	releases: boolean,
	artists: boolean,
	tracks: boolean,
	playlists: boolean,
	genres: boolean,
	libraries: boolean,
}

-- FNV-1a hash for deterministic index selection.
local FNV_OFFSET_BASIS = 0x811c9dc5
local FNV_PRIME = 0x01000193

function helpers.fnv1a_hash(input: string): number
	local hash = FNV_OFFSET_BASIS
	for i = 1, #input do
		hash = bit32.bxor(hash, string.byte(input, i))
		hash = bit32.band(hash * FNV_PRIME, 0xFFFFFFFF)
	end
	return hash
end

function helpers.deterministic_pick(sorted_ids: { number }, seed: string, count: number): { number }
	local n = #sorted_ids
	if n == 0 then
		return {}
	end
	if n <= count then
		return sorted_ids
	end

	local hash = helpers.fnv1a_hash(seed)
	local picked: { number } = {}
	local used: { [number]: boolean } = {}
	for attempt = 0, count - 1 do
		local probe = bit32.bxor(hash, attempt * FNV_PRIME)
		probe = bit32.band(probe * FNV_PRIME, 0xFFFFFFFF)
		local index = (probe % n) + 1
		-- Linear probe on collision.
		local start = index
		while used[index] do
			index = (index % n) + 1
			if index == start then
				break
			end
		end
		if not used[index] then
			used[index] = true
			table.insert(picked, sorted_ids[index])
		end
	end
	return picked
end

function helpers.deterministic_image_tag(picked_ids: { number }, seed: string): string
	local hash = helpers.fnv1a_hash(seed)
	for _, id in picked_ids do
		hash = bit32.bxor(hash, id * FNV_PRIME)
		hash = bit32.band(hash * FNV_PRIME, 0xFFFFFFFF)
	end
	return string.format("%08x", hash)
end

function helpers.compose_cover_from_ids(item_ids: { number }, seed: string): base.ResolvedCoverInfo?
	if #item_ids == 0 then
		return nil
	end

	table.sort(item_ids)
	local picked = helpers.deterministic_pick(item_ids, seed, 4)
	local cover_map = covers.get_many(picked)

	local source_paths: { string } = {}
	local first_cover: base.ResolvedCoverInfo? = nil
	for _, id in picked do
		local raw_cover = cover_map[id]
		local resolved = base.normalize_cover_info(raw_cover)
		if resolved then
			if not first_cover then
				first_cover = resolved
			end
			table.insert(source_paths, resolved.path)
		end
	end

	if #source_paths > 1 then
		local compose_ok, raw_result = pcall(
			images.compose,
			{
				sources = source_paths,
				width = 600,
				height = 600,
			} :: images.ComposeOptions
		)
		if compose_ok and raw_result then
			local result = raw_result :: images.ComposeResult
			return {
				path = result.path,
				mime_type = result.mime_type or "image/jpeg",
				hash = result.hash,
				blurhash = nil,
				release_id = nil,
			} :: base.ResolvedCoverInfo
		end
	end

	return first_cover
end

local module = {}
module.trim = base.trim
module.unquote = base.unquote
module.split_csv_preserving_quotes = base.split_csv_preserving_quotes
module.parse_query_string = base.parse_query_string
module.parse_query_int = base.parse_query_int
module.encode_jellyfin_item_id = base.encode_jellyfin_item_id
module.encode_jellyfin_db_ids = base.encode_jellyfin_db_ids
module.get_cover_info = base.get_cover_info
module.get_user_data = base.get_user_data
module.build_query_result = base.build_query_result
module.paginate_items = base.paginate_items
module.extension_from_path = base.extension_from_path
module.resolve_playback_source_for_track = base.resolve_playback_source_for_track

function module.item_to_json(item: ItemInfo): JsonObject
	return serialization.item_to_json(item)
end

function module.items_to_json(items: { ItemInfo }): { JsonValue }
	local output: { JsonValue } = {}
	for _, item in items do
		table.insert(output, serialization.item_to_json(item))
	end
	return output
end

function module.build_item_query_result(items: { ItemInfo }, total_count: number, start_index: number): JsonValue
	return module.build_query_result(module.items_to_json(items), total_count, start_index)
end

function helpers.resolve_cover_info(release_db_id: number?, context: ItemContext?): base.ResolvedCoverInfo?
	if release_db_id == nil or release_db_id <= 0 then
		return nil
	end
	if context then
		if context.cover_info_by_release_id then
			local cached = context.cover_info_by_release_id[release_db_id]
			if cached then
				return cached
			end
		end
		-- If this release was part of the prefetch set, it has no cover.
		if context.cover_prefetch_release_ids and context.cover_prefetch_release_ids[release_db_id] then
			return nil
		end
	end
	return module.get_cover_info(release_db_id)
end

function helpers.default_image_tags(): nil
	return nil
end

function helpers.default_image_blur_hashes(cover_info: { hash: string, blurhash: string? }?): ImageBlurHashes?
	local tag = if cover_info then cover_info.hash else nil
	local blurhash = if cover_info then cover_info.blurhash else nil
	if not tag or not blurhash then
		return nil
	end

	return {
		Primary = {
			[tag] = blurhash,
		},
	}
end

function helpers.should_include_user_data(context: ItemContext?): boolean
	return if context then context.include_user_data ~= false else true
end

function helpers.should_include_images(context: ItemContext?): boolean
	return if context then context.include_images ~= false else true
end

function helpers.build_item_user_data(
	item_id: number?,
	item_hex_id: string?,
	user_id: number?,
	key: string?,
	include_play_count: boolean,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): UserData?
	if not helpers.should_include_user_data(context) then
		return nil
	end

	return module.get_user_data({
		item_id = item_id,
		user_id = user_id,
		include_favorite = true,
		include_play_count = include_play_count,
		favorite_lookup = favorite_lookup,
		play_count_lookup = if context then context.play_count_lookup else nil,
		last_played_lookup = if context then context.last_played_lookup else nil,
		key = key,
		item_hex_id = item_hex_id,
	})
end

function helpers.clone_items_query_options(options: ItemsQueryOptions): ItemsQueryOptions
	return {
		include_item_types = options.include_item_types,
		ids = options.ids,
		exclude_item_ids = options.exclude_item_ids,
		exclude_artist_ids = options.exclude_artist_ids,
		primary_artist_ids = options.primary_artist_ids,
		contributing_artist_ids = options.contributing_artist_ids,
		track_artist_ids = options.track_artist_ids,
		release_artist_ids = options.release_artist_ids,
		album_ids = options.album_ids,
		artist_ids = options.artist_ids,
		genre_ids = options.genre_ids,
		filters = options.filters,
		is_favorite = options.is_favorite,
		parent_id = options.parent_id,
		start_index = options.start_index,
		limit = options.limit,
		recursive = options.recursive,
		sort_by = options.sort_by,
		sort_order = options.sort_order,
		fields = options.fields,
		search_term = options.search_term,
		enable_user_data = options.enable_user_data,
		enable_images = options.enable_images,
		include_media_info = options.include_media_info,
		include_people = options.include_people,
		group_items = options.group_items,
		user_id = options.user_id,
		_favorite_id_lookup = options._favorite_id_lookup,
		_genre_infos_by_release_id = options._genre_infos_by_release_id,
		_tag_infos_by_item_id = options._tag_infos_by_item_id,
	}
end

function helpers.split_csv(raw: string?): { string }
	local values: { string } = {}
	if type(raw) ~= "string" then
		return values
	end

	for _, part in module.split_csv_preserving_quotes(raw) do
		local item = module.trim(module.unquote(part))
		if item then
			table.insert(values, item)
		end
	end

	return values
end

function helpers.lower_or_empty(value: JsonValue): string
	if value == nil then
		return ""
	end

	return string.lower(tostring(value))
end

function helpers.normalize_sort_order(value: string?): "ascending" | "descending"
	if value ~= nil and string.lower(value) == "descending" then
		return "descending"
	end
	return "ascending"
end

function helpers.resolve_release_genres(db_id: number?): { genres.GenreInfo }?
	if db_id == nil then
		return nil
	end
	local infos = genres.get_for_release(db_id)
	if infos == nil or #infos == 0 then
		return nil
	end
	return infos
end

function helpers.genre_names(infos: { genres.GenreInfo }?): { string }
	local names: { string } = {}
	if not infos then
		return names
	end
	for _, g in infos do
		table.insert(names, g.name)
	end
	return names
end

function helpers.tag_names(infos: { TagInfo }?): { string }
	local names: { string } = {}
	local seen: { [string]: boolean } = {}
	if not infos then
		return names
	end
	for _, info in infos do
		local name = module.trim(info.tag)
		if name then
			local lookup_key = string.lower(name)
			if not seen[lookup_key] then
				seen[lookup_key] = true
				table.insert(names, name)
			end
		end
	end
	return names
end

function helpers.genre_items(infos: { genres.GenreInfo }?): { ArtistProjectionItem }
	local items: { ArtistProjectionItem } = {}
	if not infos then
		return items
	end
	for _, g in infos do
		local hex_id = base.nanoid_to_hex(g.id)
		if hex_id then
			table.insert(items, { Name = g.name, Id = hex_id })
		end
	end
	return items
end

function helpers.release_record_from_release(release: Release?): ReleaseRecord?
	if release == nil then
		return nil
	end

	return {
		id = release.id,
		db_id = release.db_id,
		release_title = release.release_title,
		sort_title = release.sort_title,
		release_date = release.release_date,
		created_at = release.created_at,
		ctime = release.ctime,
	}
end

function helpers.artist_record_from_artist(artist: Artist?): ArtistRecord?
	if artist == nil then
		return nil
	end

	return {
		id = artist.id,
		db_id = artist.db_id,
		artist_name = artist.artist_name,
		sort_name = artist.sort_name,
		created_at = artist.created_at,
	}
end

function helpers.track_record_from_track(track: Track?): TrackRecord?
	if track == nil then
		return nil
	end

	return {
		id = track.id,
		db_id = track.db_id,
		track_title = track.track_title,
		sort_title = track.sort_title,
		disc = track.disc,
		track = track.track,
		duration_ms = track.duration_ms,
		sample_rate_hz = track.sample_rate_hz,
		channel_count = track.channel_count,
		bit_depth = track.bit_depth,
		bitrate_bps = track.bitrate_bps,
		created_at = track.created_at,
		ctime = track.ctime,
	}
end

function helpers.release_records_from_releases(releases_list: { Release }?): { ReleaseRecord }?
	if releases_list == nil then
		return nil
	end

	local records: { ReleaseRecord } = {}
	for _, release: Release in releases_list do
		local record = helpers.release_record_from_release(release)
		if record then
			table.insert(records, record)
		end
	end

	return records
end

function helpers.track_records_from_tracks(track_list: { Track }?): { TrackRecord }?
	if track_list == nil then
		return nil
	end

	local records: { TrackRecord } = {}
	for _, track: Track in track_list do
		local record = helpers.track_record_from_track(track)
		if record then
			table.insert(records, record)
		end
	end

	return records
end

function helpers.artist_record_from_projection(projection: EntityProjectionInfo?): ArtistRecord?
	if projection ~= nil and projection.entity_type == "artist" then
		return helpers.artist_record_from_artist((projection :: ArtistProjectionInfo).entity)
	end
	return nil
end

function helpers.release_record_from_projection(projection: EntityProjectionInfo?): ReleaseRecord?
	if projection ~= nil and projection.entity_type == "release" then
		return helpers.release_record_from_release((projection :: ReleaseProjectionInfo).entity)
	end
	return nil
end

function helpers.track_record_from_projection(projection: EntityProjectionInfo?): TrackRecord?
	if projection ~= nil and projection.entity_type == "track" then
		return helpers.track_record_from_track((projection :: TrackProjectionInfo).entity)
	end
	return nil
end

function helpers.release_like_from_raw(raw: ItemRaw?): ReleaseLike?
	local release_record = raw :: ReleaseRecord?
	if release_record ~= nil and type(release_record.release_title) == "string" then
		return release_record
	end

	local release = raw :: Release?
	if release ~= nil and type(release.release_title) == "string" then
		return release
	end

	return nil
end

function helpers.artist_like_from_raw(raw: ItemRaw?): ArtistLike?
	local artist_record = raw :: ArtistRecord?
	if artist_record ~= nil and type(artist_record.artist_name) == "string" then
		return artist_record
	end

	local artist = raw :: Artist?
	if artist ~= nil and type(artist.artist_name) == "string" then
		return artist
	end

	return nil
end

function helpers.track_like_from_raw(raw: ItemRaw?): TrackLike?
	local track_record = raw :: TrackRecord?
	if track_record ~= nil and type(track_record.track_title) == "string" then
		return track_record
	end

	local track = raw :: Track?
	if track ~= nil and type(track.track_title) == "string" then
		return track
	end

	return nil
end

function helpers.playlist_info_from_raw(raw: ItemRaw?): PlaylistInfo?
	local playlist = raw :: PlaylistInfo?
	if playlist ~= nil and type(playlist.name) == "string" then
		return playlist
	end

	return nil
end

function helpers.query_lookup(query: ApiQueryParams?, keys: { string }): string?
	if type(query) ~= "table" then
		return nil
	end

	for _, key in keys do
		local value = query[key]
		if value ~= nil then
			return value
		end
	end

	return nil
end

function helpers.parse_query_bool(query: ApiQueryParams?, keys: { string }, default_value: boolean?): boolean?
	local value = helpers.query_lookup(query, keys)
	if value == nil then
		return default_value
	end
	if type(value) == "boolean" then
		return value
	end

	local normalized = helpers.lower_or_empty(module.trim(tostring(value)))
	if normalized == "1" or normalized == "true" or normalized == "yes" or normalized == "on" then
		return true
	end
	if normalized == "0" or normalized == "false" or normalized == "no" or normalized == "off" then
		return false
	end

	return default_value
end

function helpers.parse_query_csv(query: ApiQueryParams?, keys: { string }): { string }
	local value = helpers.query_lookup(query, keys)
	if type(value) ~= "string" then
		return {}
	end

	return helpers.split_csv(value)
end

function helpers.decode_jellyfin_item_id(value: JsonValue): number?
	if value == nil then
		return nil
	end

	if type(value) == "number" then
		local numeric = math.floor(value)
		if numeric <= 0 then
			return nil
		end
		return numeric
	end

	if type(value) == "string" then
		return base.decode_jellyfin_item_id(value)
	end

	return nil
end

function helpers.parse_query_id(query: ApiQueryParams?, keys: { string }): string?
	return module.parse_query_string(query, keys, nil)
end

function helpers.ms_to_ticks(duration_ms: number | string?): number?
	local value = tonumber(duration_ms)
	if not value then
		return nil
	end

	return math.floor(value * 10000)
end

function helpers.unix_to_iso8601(unix_seconds: number | string?): string?
	local s = tonumber(unix_seconds)
	if not s then
		return nil
	end
	s = math.floor(s)
	local z = math.floor(s / 86400) + 719468
	local era = math.floor((if z >= 0 then z else z - 146096) / 146097)
	local doe = z - era * 146097
	local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524) - math.floor(doe / 146096)) / 365)
	local y = yoe + era * 400
	local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
	local mp = math.floor((5 * doy + 2) / 153)
	local d = doy - math.floor((153 * mp + 2) / 5) + 1
	local m = if mp < 10 then mp + 3 else mp - 9
	if m <= 2 then
		y += 1
	end
	local rem = s % 86400
	return string.format(
		"%04d-%02d-%02dT%02d:%02d:%02d.0000000Z",
		y,
		m,
		d,
		math.floor(rem / 3600),
		math.floor((rem % 3600) / 60),
		rem % 60
	)
end

function helpers.release_year(release_date: string?): number?
	if not release_date or string.len(release_date) < 4 then
		return nil
	end
	return tonumber(string.sub(release_date, 1, 4))
end

function helpers.release_date_to_premiere_date(release_date: string?): string?
	local y = helpers.release_year(release_date)
	if not y or not release_date then
		return nil
	end
	local month = tonumber(string.sub(release_date, 6, 7)) or 1
	local day = tonumber(string.sub(release_date, 9, 10)) or 1
	return string.format("%04d-%02d-%02dT00:00:00.0000000Z", math.floor(y), math.floor(month), math.floor(day))
end

function helpers.first_or_nil<T>(items: { T }?): T?
	if type(items) ~= "table" or #items == 0 then
		return nil
	end

	return items[1]
end

function helpers.resolve_library_hex_id_from_parent(parent_id: string?): string?
	if parent_id == nil then
		return nil
	end
	local parent_text = helpers.lower_or_empty(parent_id)
	if helpers.is_library_root(parent_text) then
		return parent_text
	end
	return nil
end

function helpers.resolve_library_hex_id_for_entity(entity_db_id: number?): string?
	if not entity_db_id or entity_db_id <= 0 then
		return nil
	end
	local ok, libs = pcall(libraries.get_for_entity, entity_db_id)
	if ok and libs and #libs > 0 then
		local first = libs[1] :: libraries.Library
		if first.id then
			return base.nanoid_to_hex(first.id)
		end
	end
	return nil
end

function helpers.resolve_entity_library_hex_id(entity_db_id: number?, context: ItemContext?): string?
	if context and context.library_hex_id then
		return context.library_hex_id
	end
	if entity_db_id and context and context.library_hex_id_by_entity_id then
		local cached = context.library_hex_id_by_entity_id[entity_db_id]
		if cached then
			return cached
		end
	end
	return helpers.resolve_library_hex_id_for_entity(entity_db_id)
end

function helpers.normalize_parent_scope(parent_id: string?): number?
	if parent_id == nil then
		return nil
	end

	local parent_text = helpers.lower_or_empty(parent_id)
	if parent_text == "" then
		return nil
	end

	return helpers.decode_jellyfin_item_id(parent_id)
end

function helpers.scope_entity_type(scope: number?): string?
	if scope == nil or scope <= 0 then
		return nil
	end
	local ok, element_type = pcall(entities.get_type, scope)
	return if ok and type(element_type) == "string" then element_type else nil
end

function helpers.resolve_parent_scope(parent_id: string?): ParentScopeInfo
	local db_id = helpers.normalize_parent_scope(parent_id)
	return {
		db_id = db_id,
		entity_type = helpers.scope_entity_type(db_id),
	}
end

function helpers.is_library_scope(scope: number?): boolean
	return helpers.scope_entity_type(scope) == "Library"
end

function helpers.call_module_get<T, U>(get_fn: (U?) -> { T }, scope: U?): { T }
	local ok: boolean
	local value: { T }?
	if scope ~= nil then
		ok, value = pcall(get_fn, scope)
	else
		ok, value = pcall(get_fn, nil)
	end
	if not ok then
		return {}
	end

	return value or {}
end

function helpers.resolve_numeric_id(value: number?): number?
	if value == nil then
		return nil
	end
	local numeric = math.floor(value)
	if numeric > 0 then
		return numeric
	end
	return nil
end

function helpers.lookup_by_id<T>(lookup: { [number]: T }?, id: number?): T?
	if type(lookup) ~= "table" or id == nil then
		return nil
	end
	return lookup[id]
end

function helpers.append_unique_id(id: number?, output: { number }, seen: { [number]: boolean }): number?
	if id == nil or id <= 0 then
		return nil
	end

	if not seen[id] then
		seen[id] = true
		table.insert(output, id)
	end
	return id
end

function helpers.normalize_tag_info_map(target_ids: { number }, raw_tag_map: TagInfoMap?): TagInfoMap
	local mapped: TagInfoMap = {}
	for _, target_id: number in target_ids do
		mapped[target_id] = {}
	end
	if type(raw_tag_map) ~= "table" then
		return mapped
	end

	for key, infos in raw_tag_map do
		local target_id = helpers.resolve_numeric_id(key)
		if target_id and type(infos) == "table" then
			mapped[target_id] = infos
		end
	end
	return mapped
end

function helpers.fetch_tag_info_map(user_id: number?, target_ids: { number }): TagInfoMap
	if #target_ids == 0 then
		return {}
	end
	if user_id == nil or user_id <= 0 then
		return helpers.normalize_tag_info_map(target_ids, nil)
	end

	local ok, raw_tag_map = pcall(tags_module.get_for_targets_many, user_id, target_ids)
	if not ok or type(raw_tag_map) ~= "table" then
		return helpers.normalize_tag_info_map(target_ids, nil)
	end

	return helpers.normalize_tag_info_map(target_ids, raw_tag_map :: TagInfoMap)
end

function helpers.fetch_tag_infos(user_id: number?, target_id: number?): { TagInfo }?
	local resolved_id = helpers.resolve_numeric_id(target_id)
	if user_id == nil or user_id <= 0 or resolved_id == nil then
		return nil
	end

	local ok, raw_tags = pcall(tags_module.get_for_target, user_id, resolved_id)
	if not ok or type(raw_tags) ~= "table" then
		return nil
	end

	return raw_tags :: { TagInfo }
end

function helpers.resolve_tag_infos(target_id: number?, context: ItemContext?): { TagInfo }?
	local resolved_id = helpers.resolve_numeric_id(target_id)
	if resolved_id == nil then
		return nil
	end

	if context and context.tag_infos_by_item_id then
		local cached = context.tag_infos_by_item_id[resolved_id]
		if type(cached) == "table" then
			return cached
		end
	end

	return helpers.fetch_tag_infos(if context then context.user_id else nil, resolved_id)
end

function helpers.query_many_projections(
	ids: { number },
	include_values: { string }?
): { [string]: EntityProjectionInfo }
	if #ids == 0 then
		return {}
	end
	if type(entities.query_many) ~= "function" then
		return {}
	end

	local request: EntityQueryManyRequest = { ids = {} }
	for _, id: number in ids do
		table.insert(request.ids, id)
	end
	if type(include_values) == "table" and #include_values > 0 then
		request.include = include_values
	end

	local ok, projections = pcall(entities.query_many, request)
	if not ok or type(projections) ~= "table" then
		return {}
	end

	return projections
end

function helpers.projection_credits(projection: EntityProjectionInfo?): { CreditedArtistProjectionInfo }?
	if projection == nil then
		return nil
	end

	if projection.entity_type == "release" then
		return (projection :: ReleaseProjectionInfo).includes.credits
	elseif projection.entity_type == "track" then
		return (projection :: TrackProjectionInfo).includes.credits
	end

	return nil
end

function helpers.query_credits(entity_id: number, entity_type: "release" | "track"): { CreditedArtistProjectionInfo }?
	local request: EntityQueryRequest = {
		id = entity_id,
		include = "credits",
	}
	local ok, projection = pcall(entities.query, request)
	if not ok or type(projection) ~= "table" then
		return nil
	end

	local typed_projection = projection :: EntityProjectionInfo
	if typed_projection.entity_type ~= entity_type then
		return nil
	end

	return helpers.projection_credits(typed_projection)
end

function helpers.merge_artist_relation_map(destination: ArtistRelationMap, relation_map: { [number]: { Artist } })
	for key: number, value: { Artist } in relation_map do
		local relation_id = helpers.resolve_numeric_id(key)
		if relation_id ~= nil and type(value) == "table" then
			destination[relation_id] = value
		end
	end
end

function helpers.merge_release_relation_map(destination: ReleaseRelationMap, relation_map: { [number]: { Release } })
	for key: number, value: { Release } in relation_map do
		local relation_id = helpers.resolve_numeric_id(key)
		if relation_id ~= nil and type(value) == "table" then
			destination[relation_id] = value
		end
	end
end

function helpers.merge_track_relation_map(destination: TrackRelationMap, relation_map: { [number]: { Track } })
	for key: number, value: { Track } in relation_map do
		local relation_id = helpers.resolve_numeric_id(key)
		if relation_id ~= nil and type(value) == "table" then
			destination[relation_id] = value
		end
	end
end

function helpers.ensure_relation_entries<T>(relation_map: { [number]: { T }? }, ids: { number })
	for _, id: number in ids do
		if type(relation_map[id]) ~= "table" then
			relation_map[id] = {}
		end
	end
end

function helpers._relation_entry_has_values<T>(relation_map: { [number]: { T }? }, id: number): boolean
	local value = relation_map[id]
	return type(value) == "table" and #value > 0
end

function helpers.relation_map_has_entries<T>(relation_map: { [number]: { T } }): boolean
	for _, value in relation_map do
		if type(value) == "table" and #value > 0 then
			return true
		end
	end

	return false
end

function helpers.decode_artist_filter_ids(raw_artist_ids: { string }): { number }
	local decoded: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, raw_id in raw_artist_ids do
		local artist_id = helpers.decode_jellyfin_item_id(raw_id)
		if artist_id ~= nil and not seen[artist_id] then
			seen[artist_id] = true
			table.insert(decoded, artist_id)
		end
	end

	return decoded
end

function helpers.decode_item_filter_ids(raw_ids: { string }): { number }
	local decoded: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, raw_id in raw_ids do
		local item_id = helpers.decode_jellyfin_item_id(raw_id)
		if item_id ~= nil and not seen[item_id] then
			seen[item_id] = true
			table.insert(decoded, item_id)
		end
	end

	return decoded
end

function helpers.merge_numeric_id_lists(first: { number }, second: { number }): { number }
	local merged: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, id in first do
		if not seen[id] then
			seen[id] = true
			table.insert(merged, id)
		end
	end
	for _, id in second do
		if not seen[id] then
			seen[id] = true
			table.insert(merged, id)
		end
	end
	return merged
end

function helpers.codec_for_extension(extension: string?): string?
	if extension == "flac" then
		return "flac"
	end
	if extension == "mp3" then
		return "mp3"
	end
	if extension == "aac" or extension == "m4a" or extension == "mp4" then
		return "aac"
	end
	if extension == "opus" then
		return "opus"
	end
	if extension == "ogg" then
		return "vorbis"
	end
	if extension == "wav" then
		return "pcm_s16le"
	end
	if extension == "alac" then
		return "alac"
	end

	return nil
end

function helpers.sum_runtime_ticks(track_items: { Track }?): number?
	if track_items == nil then
		return nil
	end

	local total_ticks = 0
	local has_value = false
	for _, track: Track in track_items do
		local duration_ticks = helpers.ms_to_ticks(track.duration_ms)
		if duration_ticks then
			total_ticks += duration_ticks
			has_value = true
		end
	end

	if not has_value then
		return nil
	end

	return total_ticks
end

function helpers.build_media_info(
	track: TrackLike?,
	source: PlaybackSourceInfo?,
	item_id: string?,
	fallback_name: string?
): ({ MediaSourceInfo }, { MediaStreamInfo }, string?)
	local entry = if source then source.entry else nil
	local media_item_id = module.encode_jellyfin_item_id(item_id)
	local container = module.extension_from_path(if entry then entry.full_path else nil)
	local codec = helpers.codec_for_extension(container)
	local size = if entry and entry.size then tonumber(entry.size) else nil
	local bitrate = if track then track.bitrate_bps else nil
	local full_path = if entry then module.trim(entry.full_path) else nil
	local direct_stream_url = if full_path and media_item_id then `{ROUTE_PREFIX}/Items/{media_item_id}/File` else nil

	local media_stream: MediaStreamInfo = {
		Codec = codec,
		Type = "Audio",
		Index = 0,
		Channels = if track then track.channel_count else nil,
		SampleRate = if track then track.sample_rate_hz else nil,
		BitDepth = if track then track.bit_depth else nil,
		IsDefault = true,
		IsForced = false,
		IsInterlaced = false,
		IsExternal = false,
		IsTextSubtitleStream = false,
		SupportsExternalStream = false,
	}
	if bitrate then
		media_stream.BitRate = bitrate
	end
	local media_streams: { MediaStreamInfo } = { media_stream }
	local media_stream_values: { JsonValue } = { media_stream }

	local media_source: MediaSourceInfo = {
		Protocol = "File",
		Id = media_item_id,
		Path = if entry then entry.full_path else nil,
		Type = "Default",
		Container = container,
		Size = size,
		Name = if entry
			then (entry.name or fallback_name or tostring(item_id))
			else (fallback_name or tostring(item_id)),
		IsRemote = false,
		RunTimeTicks = if track then helpers.ms_to_ticks(track.duration_ms) else nil,
		ReadAtNativeFramerate = false,
		IgnoreDts = false,
		IgnoreIndex = false,
		GenPtsInput = false,
		SupportsTranscoding = false,
		SupportsDirectStream = direct_stream_url ~= nil,
		SupportsDirectPlay = direct_stream_url ~= nil,
		IsInfiniteStream = false,
		RequiresOpening = false,
		RequiresClosing = false,
		RequiresLooping = false,
		SupportsProbing = false,
		MediaStreams = media_stream_values,
		Bitrate = bitrate,
		DefaultAudioStreamIndex = 0,
		HasSegments = false,
	}
	if entry and entry.hash then
		media_source.ETag = tostring(entry.hash)
	end
	if direct_stream_url then
		media_source.DirectStreamUrl = direct_stream_url
	end

	return { media_source }, media_streams, container
end

function helpers.resolve_track_container(track: TrackLike?): string?
	local track_id = if track then track.db_id else nil
	if track_id == nil then
		return nil
	end

	local container = track_sources.get_primary_container(track_id)
	return module.trim(container)
end

function helpers.contains_text(haystack: JsonValue, needle: string?): boolean
	if not needle or needle == "" then
		return true
	end

	return string.find(helpers.lower_or_empty(haystack), helpers.lower_or_empty(needle), 1, true) ~= nil
end

function module.apply_search_filter(items: { ItemInfo }, search_term: string?): { ItemInfo }
	if not search_term then
		return items
	end

	local filtered: { ItemInfo } = {}
	for _, item: ItemInfo in items do
		if helpers.contains_text(item.Name, search_term) then
			table.insert(filtered, item)
		end
	end

	return filtered
end

function helpers.build_artist_projection(related_artists: { Artist }?): (string?, { string }, { ArtistProjectionItem })
	local names: { string } = {}
	local items: { ArtistProjectionItem } = {}
	local index = 1
	if related_artists == nil then
		return nil, names, items
	end

	for _, artist: Artist in related_artists do
		local artist_name = module.trim(artist.artist_name)
		if artist_name then
			local artist_id = if artist and artist.id then module.encode_jellyfin_item_id(artist.id) else nil

			names[index] = artist_name
			if artist_id then
				items[#items + 1] = {
					Id = artist_id,
					Name = artist_name,
				}
			end
			index += 1
		end
	end

	return names[1], names, items
end

function helpers.person_type_for_credit(credit_type: CreditType, source: ArtistCreditSource): PersonType
	if credit_type == CreditType.Artist then
		return if source == ArtistCreditSource.Release then "AlbumArtist" else "Artist"
	elseif credit_type == CreditType.Vocalist or credit_type == CreditType.Instrumentalist then
		return "Actor"
	elseif credit_type == CreditType.Composer then
		return "Composer"
	elseif credit_type == CreditType.Lyricist then
		return "Lyricist"
	elseif credit_type == CreditType.Arranger then
		return "Arranger"
	elseif credit_type == CreditType.Writer then
		return "Writer"
	elseif credit_type == CreditType.Producer then
		return "Producer"
	elseif credit_type == CreditType.Conductor then
		return "Conductor"
	elseif credit_type == CreditType.Engineer then
		return "Engineer"
	elseif credit_type == CreditType.Mixer then
		return "Mixer"
	elseif credit_type == CreditType.Remixer then
		return "Remixer"
	end
	return "Unknown"
end

function helpers.people_from_credits(credits: { CreditedArtistProjectionInfo }?): { PersonItem }
	local people: { PersonItem } = {}
	local seen: { [string]: boolean } = {}
	if credits == nil then
		return people
	end

	for _, credited_artist: CreditedArtistProjectionInfo in credits do
		local artist = credited_artist.artist
		local artist_name = module.trim(artist.artist_name)
		local artist_id = module.encode_jellyfin_item_id(artist.id)
		if artist_name == nil or artist_id == nil then
			continue
		end

		local credit = credited_artist.credit
		local person_type = helpers.person_type_for_credit(credit.type, credited_artist.source)
		local role = module.trim(credit.detail)
		local key = artist_id .. "\t" .. person_type .. "\t" .. (role or "")
		if seen[key] then
			continue
		end
		seen[key] = true

		local person: PersonItem = {
			Name = artist_name,
			Id = artist_id,
			Role = role,
			Type = person_type,
		}
		table.insert(people, person)
	end

	return people
end

function helpers.resolve_people_for_item(
	entity_id: number?,
	entity_type: "release" | "track",
	context: ItemContext?
): { PersonItem }?
	if context == nil or context.include_people ~= true then
		return nil
	end
	if entity_id == nil or entity_id <= 0 then
		return {}
	end

	local credits = helpers.lookup_by_id(context.credited_artists_by_owner_id, entity_id)
	if credits == nil then
		credits = helpers.query_credits(entity_id, entity_type)
		if credits ~= nil then
			context.credited_artists_by_owner_id[entity_id] = credits
		end
	end

	return helpers.people_from_credits(credits)
end

function helpers.collect_artist_projection(
	entity_id: number?,
	entity_type: "release" | "track",
	context: ItemContext?
): (string?, { string }, { ArtistProjectionItem })
	local names: { string } = {}
	local items: { ArtistProjectionItem } = {}
	if entity_id == nil then
		return nil, names, items
	end

	local credits = helpers.lookup_by_id(if context then context.credited_artists_by_owner_id else nil, entity_id)
	if credits == nil then
		credits = helpers.query_credits(entity_id, entity_type)
	end
	if credits == nil then
		return nil, names, items
	end

	local seen: { [string]: boolean } = {}
	for _, credited_artist: CreditedArtistProjectionInfo in credits do
		local credit = credited_artist.credit
		if credit.type ~= CreditType.Artist then
			continue
		end
		local artist = credited_artist.artist
		local artist_name = module.trim(artist.artist_name)
		local artist_id = module.encode_jellyfin_item_id(artist.id)
		if artist_name == nil or artist_id == nil or seen[artist_id] then
			continue
		end
		seen[artist_id] = true
		table.insert(names, artist_name)
		table.insert(items, {
			Id = artist_id,
			Name = artist_name,
		})
	end

	return names[1], names, items
end

function helpers.to_artist_item(
	artist: ArtistLike?,
	user_id: number?,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): ItemInfo?
	if not artist or not artist.db_id then
		return nil
	end

	local sort_name = module.trim(artist.sort_name) or module.trim(artist.artist_name) or "Unknown Artist"
	local artist_item_id = module.encode_jellyfin_item_id(artist.id)
	local artist_tracks =
		helpers.lookup_by_id(if context then context.artist_tracks_by_artist_id else nil, artist.db_id)
	if artist_tracks == nil then
		artist_tracks = helpers.call_module_get(tracks.list, artist.db_id)
	end
	local runtime_ticks = helpers.sum_runtime_ticks(artist_tracks)
	local artist_releases = releases.get_by_artist(artist.db_id)
	local album_count = if artist_releases then #artist_releases else 0
	local item: ItemInfo = {
		Id = artist_item_id,
		Name = module.trim(artist.artist_name) or "Unknown Artist",
		Type = "MusicArtist",
		ParentId = helpers.resolve_entity_library_hex_id(artist.db_id, context),
		SortName = sort_name,
		RunTimeTicks = runtime_ticks,
		DateCreated = helpers.unix_to_iso8601(artist.created_at),
		AlbumCount = album_count,
		SongCount = if artist_tracks then #artist_tracks else 0,
		ImageTags = helpers.default_image_tags(),
		ImageBlurHashes = helpers.default_image_blur_hashes(),
		UserData = helpers.build_item_user_data(
			artist.db_id,
			artist_item_id,
			user_id,
			"Artist-" .. (module.trim(artist.artist_name) or ""),
			true,
			favorite_lookup,
			context
		),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		Tags = helpers.tag_names(helpers.resolve_tag_infos(artist.db_id, context)),
	}
	return item
end

type GenreCounts = {
	release_count: number,
	song_count: number,
	artist_count: number,
}

function helpers.to_genre_item(
	genre_name: string?,
	genre_id: string?,
	runtime_ticks: number?,
	counts: GenreCounts?,
	library_hex_id: string?,
	image_tag: string?
): ItemInfo?
	local safe_name = module.trim(genre_name)
	if not safe_name or not genre_id then
		return nil
	end

	local hex_id = base.nanoid_to_hex(genre_id)
	if not hex_id then
		return nil
	end

	local item: ItemInfo = {
		Id = hex_id,
		Name = safe_name,
		Type = "MusicGenre",
		IsFolder = true,
		ParentId = library_hex_id,
		SortName = safe_name,
		RunTimeTicks = runtime_ticks,
		AlbumCount = if counts then counts.release_count else nil,
		SongCount = if counts then counts.song_count else nil,
		ArtistCount = if counts then counts.artist_count else nil,
		ChildCount = if counts then counts.release_count + counts.song_count + counts.artist_count else nil,
		UserData = module.get_user_data({ key = "MusicGenre-" .. safe_name, item_hex_id = hex_id }),
		ImageTags = if image_tag then { Primary = image_tag } else nil,
		ImageBlurHashes = nil,
		LocationType = "FileSystem",
		MediaType = "Unknown",
	}
	return item
end

function helpers.to_library_view_item(lib: LibraryInfo?): ItemInfo?
	if not lib then
		return nil
	end
	local hex_id = base.nanoid_to_hex(lib.id)
	if not hex_id then
		return nil
	end

	return {
		Id = hex_id,
		Name = lib.name,
		Type = "CollectionFolder",
		CollectionType = "music",
		IsFolder = true,
		UserData = module.get_user_data({ key = "CollectionFolder-" .. hex_id, item_hex_id = hex_id }),
		LocationType = "FileSystem",
		MediaType = "Unknown",
	} :: ItemInfo
end

function helpers.find_library_view_item_by_id(library_db_id: number): ItemInfo?
	local ok, libs = pcall(libraries.list, library_db_id)
	if not ok or type(libs) ~= "table" then
		return nil
	end

	return helpers.to_library_view_item(helpers.first_or_nil(libs :: { LibraryInfo }))
end

function module.to_music_view_items(): { ItemInfo }
	local ok, libs = pcall(libraries.list, nil)
	if not ok or type(libs) ~= "table" then
		return {}
	end
	local items: { ItemInfo } = {}
	for _, lib in libs :: { LibraryInfo } do
		local item = helpers.to_library_view_item(lib)
		if item then
			table.insert(items, item)
		end
	end
	return items
end

function module.find_playlist_by_id(playlist_id: number?): PlaylistInfo?
	if not playlist_id or playlist_id <= 0 then
		return nil
	end

	local ok, result = pcall(playlists.get_by_id, playlist_id)
	if not ok then
		return nil
	end

	return result
end

function module.playlist_owner_id(playlist: PlaylistInfo?): number?
	local raw_playlist_id = if playlist then playlist.db_id else nil
	if raw_playlist_id == nil or raw_playlist_id <= 0 then
		return nil
	end

	local ok, owner_id = pcall(playlists.get_owner, raw_playlist_id)
	if not ok then
		return nil
	end

	if owner_id == nil or type(owner_id) ~= "number" or owner_id <= 0 then
		return nil
	end

	return owner_id
end

function module.playlist_can_edit(playlist, user_id)
	local owner_id = module.playlist_owner_id(playlist)
	return owner_id ~= nil and user_id ~= nil and owner_id == user_id
end

function module.playlist_can_view(playlist, user_id)
	if module.playlist_can_edit(playlist, user_id) then
		return true
	end

	return playlist ~= nil and playlist.is_public == true
end

function helpers.list_user_playlists(user_id: number?): { PlaylistInfo }
	if user_id == nil or user_id <= 0 then
		return {}
	end
	local ok, playlist_items = pcall(playlists.get_by_user, user_id)
	if not ok or type(playlist_items) ~= "table" then
		return {}
	end
	return playlist_items
end

function module.playlist_track_refs(playlist: PlaylistInfo?): { PlaylistTrackLink }
	local playlist_id = if playlist then playlist.db_id else nil
	if playlist_id == nil or playlist_id <= 0 then
		return {}
	end
	local ok, track_links = pcall(playlists.get_tracks, playlist_id)
	if not ok or type(track_links) ~= "table" then
		return {}
	end
	return track_links
end

function helpers.playlist_track_count(playlist: PlaylistInfo?): number
	return #module.playlist_track_refs(playlist)
end

function helpers.get_playlist_track_ids(playlist: PlaylistInfo): { number }
	local playlist_db_id = playlist.db_id
	if not playlist_db_id or playlist_db_id <= 0 then
		return {}
	end

	local ok, raw_track_links = pcall(playlists.get_tracks, playlist_db_id)
	if not ok or not raw_track_links then
		return {}
	end
	local track_links = raw_track_links :: { PlaylistTrackLink }

	local track_ids: { number } = {}
	for _, link: PlaylistTrackLink in track_links do
		if link.track_id and link.track_id > 0 then
			table.insert(track_ids, link.track_id)
		end
	end
	return track_ids
end

function helpers.resolve_playlist_cover(playlist: PlaylistInfo): base.ResolvedCoverInfo?
	local track_ids = helpers.get_playlist_track_ids(playlist)
	return helpers.compose_cover_from_ids(track_ids, playlist.id or "")
end

function helpers.to_playlist_item(
	playlist: PlaylistInfo?,
	user_id: number?,
	child_count: number?,
	context: ItemContext?
): ItemInfo?
	if not playlist or not playlist.db_id then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local playlist_name = module.trim(playlist.name) or "Untitled Playlist"
	local playlist_item_id = module.encode_jellyfin_item_id(playlist.id)

	-- Use pre-fetched track IDs from context if available, otherwise fetch.
	local track_ids: { number }
	local prefetched = if context
			and context.playlist_track_ids_by_playlist_id
			and playlist.db_id
		then context.playlist_track_ids_by_playlist_id[playlist.db_id]
		else nil
	if prefetched then
		track_ids = prefetched
	else
		track_ids = helpers.get_playlist_track_ids(playlist)
	end
	local count = if child_count ~= nil then child_count else #track_ids

	local image_tag: string? = nil
	if include_images and #track_ids > 0 then
		table.sort(track_ids)
		local picked = helpers.deterministic_pick(track_ids, playlist.id or "", 4)
		image_tag = helpers.deterministic_image_tag(picked, playlist.id or "")
	end

	local item: ItemInfo = {
		Id = playlist_item_id,
		Name = playlist_name,
		Type = "Playlist",
		SortName = playlist_name,
		ChildCount = count,
		DateCreated = helpers.unix_to_iso8601(playlist.created_at),
		ImageTags = if image_tag then { Primary = image_tag } else nil,
		ImageBlurHashes = nil,
		UserData = helpers.build_item_user_data(
			playlist.db_id,
			playlist_item_id,
			user_id,
			"Playlist-" .. (playlist.name or ""),
			false,
			if context then context.favorite_lookup else nil,
			context
		),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		Tags = helpers.tag_names(helpers.resolve_tag_infos(playlist.db_id, context)),
	}
	return item
end

function helpers.to_release_item(
	release: ReleaseLike?,
	user_id: number?,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): ItemInfo?
	if not release or not release.db_id then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local release_artist, artist_names, artist_items = helpers.collect_artist_projection(release.db_id, "release", context)
	local prefetched_release_tracks =
		helpers.lookup_by_id(if context then context.release_tracks_by_release_id else nil, release.db_id)
	local release_tracks: { Track } = if prefetched_release_tracks ~= nil
		then prefetched_release_tracks
		else helpers.call_module_get(tracks.list, release.db_id)
	local runtime_ticks = helpers.sum_runtime_ticks(release_tracks)
	-- Jellyfin exposes image tags by default, but honors EnableImages=false.
	local cover_info = if include_images then helpers.resolve_cover_info(release.db_id, context) else nil
	local release_item_id = module.encode_jellyfin_item_id(release.id)
	local image_resource_id = if cover_info then module.encode_jellyfin_item_id(release.id) else nil
	local release_genres = helpers.lookup_by_id(
		if context then context.genre_infos_by_release_id else nil,
		release.db_id
	) or helpers.resolve_release_genres(release.db_id)
	local item: ItemInfo = {
		Id = release_item_id,
		Name = module.trim(release.release_title) or "Unknown Album",
		Type = "MusicAlbum",
		IsFolder = true,
		MediaType = "Unknown",
		SortName = module.trim(release.sort_title) or module.trim(release.release_title) or "Unknown Album",
		ProductionYear = helpers.release_year(release.release_date),
		PremiereDate = helpers.release_date_to_premiere_date(release.release_date),
		DateCreated = helpers.unix_to_iso8601(release.ctime or release.created_at),
		Genres = helpers.genre_names(release_genres),
		GenreItems = helpers.genre_items(release_genres),
		Tags = helpers.tag_names(helpers.resolve_tag_infos(release.db_id, context)),
		ParentId = if #artist_items > 0
			then artist_items[1].Id
			else helpers.resolve_entity_library_hex_id(release.db_id, context),
		AlbumArtist = release_artist,
		AlbumArtists = artist_items,
		Artists = artist_names,
		ArtistItems = artist_items,
		People = helpers.resolve_people_for_item(release.db_id, "release", context),
		ChildCount = #release_tracks,
		RunTimeTicks = runtime_ticks,
		ImageTags = if cover_info then { Primary = cover_info.hash } else nil,
		ParentPrimaryImageItemId = image_resource_id,
		ParentPrimaryImageTag = if cover_info then cover_info.hash else nil,
		AlbumPrimaryImageTag = if cover_info then cover_info.hash else nil,
		ImageBlurHashes = helpers.default_image_blur_hashes(cover_info),
		UserData = helpers.build_item_user_data(
			release.db_id,
			release_item_id,
			user_id,
			(release_artist or "") .. "-" .. (module.trim(release.release_title) or ""),
			true,
			favorite_lookup,
			context
		),
		LocationType = "FileSystem",
	}
	return item
end

function helpers.resolve_release_id_number(release: Release?): number?
	local release_id = if release then release.db_id else nil
	if release_id == nil or release_id <= 0 then
		return nil
	end
	return release_id
end

function helpers.resolve_release_for_track(
	track: TrackLike?,
	preferred_release_id: number?,
	include_images: boolean?,
	context: ItemContext?
): (Release?, CoverInfo?)
	local track_id = if track then track.db_id else nil
	local prefetched_releases =
		helpers.lookup_by_id(if context then context.track_releases_by_track_id else nil, track_id)
	local raw_releases: { Release } = if prefetched_releases ~= nil then prefetched_releases else {}
	local has_complete_lookup = if context then context.track_release_lookup_complete == true else false

	local should_fallback = prefetched_releases == nil
	if not should_fallback and #raw_releases == 0 then
		-- When images are requested, allow a targeted fallback so track items
		-- still populate release-cover fields even if relation prefetch missed.
		should_fallback = (not has_complete_lookup) or include_images == true
	end

	if should_fallback then
		-- Fallback when bulk relation prefetch misses this track.
		raw_releases = helpers.call_module_get(releases.list, track_id)
	end
	if #raw_releases == 0 then
		return nil, nil
	end

	local candidates: { Release } = {}
	for _, release: Release in raw_releases do
		if helpers.resolve_release_id_number(release) ~= nil then
			table.insert(candidates, release)
		end
	end
	if #candidates == 0 then
		return nil, nil
	end

	table.sort(candidates, function(a: Release, b: Release): boolean
		local a_id = helpers.resolve_release_id_number(a) or 0
		local b_id = helpers.resolve_release_id_number(b) or 0
		return a_id < b_id
	end)

	local preferred_release_id_number = helpers.resolve_numeric_id(preferred_release_id)
	if preferred_release_id_number ~= nil then
		for _, release in candidates do
			local release_id = helpers.resolve_release_id_number(release)
			if release_id == preferred_release_id_number then
				local preferred_cover = if include_images
					then helpers.resolve_cover_info(release.db_id, context)
					else nil
				return release, preferred_cover
			end
		end
	end

	local selected_release: Release? = candidates[1]
	if include_images ~= true then
		return selected_release, nil
	end

	local selected_cover_info = if selected_release
		then helpers.resolve_cover_info(selected_release.db_id, context)
		else nil
	for _, release: Release in candidates do
		local cover_info = helpers.resolve_cover_info(release.db_id, context)
		if cover_info then
			selected_release = release
			selected_cover_info = cover_info
			break
		end
	end

	return selected_release, selected_cover_info
end

function helpers.to_track_item(
	track: TrackLike?,
	preferred_release_id: number?,
	user_id: number?,
	favorite_lookup: FavoriteLookup?,
	include_media_info: boolean?,
	context: ItemContext?
): ItemInfo?
	if not track or not track.db_id then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local release, release_cover_info =
		helpers.resolve_release_for_track(track, preferred_release_id, include_images, context)
	local artist_name, artist_names, artist_items = helpers.collect_artist_projection(track.db_id, "track", context)
	local release_artist_name = artist_name
	local release_artist_names = artist_names
	local release_artist_items = artist_items
	if release and release.db_id then
		local resolved_release_artist_name, resolved_release_artist_names, resolved_release_artist_items =
			helpers.collect_artist_projection(release.db_id, "release", context)
		if resolved_release_artist_name then
			release_artist_name = resolved_release_artist_name
		end
		if #resolved_release_artist_names > 0 then
			release_artist_names = resolved_release_artist_names
		end
		if #resolved_release_artist_items > 0 then
			release_artist_items = resolved_release_artist_items
		end
		if artist_name == nil and #release_artist_names > 0 then
			artist_name = release_artist_name
			artist_names = release_artist_names
			artist_items = release_artist_items
		end
	end
	local media_sources: { MediaSourceInfo }? = nil
	local media_streams: { MediaStreamInfo }? = nil
	local container = nil
	if include_media_info == true then
		local source = nil
		if context and context.has_source_prefetch then
			source = helpers.lookup_by_id(context.playback_source_by_track_id, track.db_id)
		else
			source = module.resolve_playback_source_for_track(track, true)
		end
		media_sources, media_streams, container = helpers.build_media_info(track, source, track.id, track.track_title)
	else
		if context and context.has_container_prefetch then
			container = module.trim(helpers.lookup_by_id(context.container_by_track_id, track.db_id))
		else
			container = helpers.resolve_track_container(track)
		end
	end
	local release_id = if release and release.id then module.encode_jellyfin_item_id(release.id) else nil
	local track_item_id = module.encode_jellyfin_item_id(track.id)
	local parent_primary_image_item_id = if release and release_cover_info
		then module.encode_jellyfin_item_id(release.id)
		else nil
	local parent_primary_image_tag = if release_cover_info then release_cover_info.hash else nil
	local release_db_id = if release then release.db_id else nil
	local track_genres = helpers.lookup_by_id(if context then context.genre_infos_by_release_id else nil, release_db_id)
		or helpers.resolve_release_genres(release_db_id)

	local item: ItemInfo = {
		Id = track_item_id,
		Name = module.trim(track.track_title) or "Unknown Track",
		Type = "Audio",
		IsFolder = false,
		MediaType = "Audio",
		LocationType = "FileSystem",
		Container = container,
		SortName = module.trim(track.sort_title) or module.trim(track.track_title) or "Unknown Track",
		RunTimeTicks = helpers.ms_to_ticks(track.duration_ms),
		DateCreated = helpers.unix_to_iso8601(track.ctime or track.created_at),
		IndexNumber = track.track,
		ParentIndexNumber = track.disc,
		Album = if release then release.release_title else nil,
		AlbumId = release_id,
		ParentId = release_id,
		Artist = artist_name,
		AlbumArtist = release_artist_name,
		AlbumArtists = release_artist_items,
		Artists = artist_names,
		ArtistItems = artist_items,
		People = helpers.resolve_people_for_item(track.db_id, "track", context),
		Genres = helpers.genre_names(track_genres),
		GenreItems = helpers.genre_items(track_genres),
		Tags = helpers.tag_names(helpers.resolve_tag_infos(track.db_id, context)),
		ParentPrimaryImageItemId = parent_primary_image_item_id,
		ParentPrimaryImageTag = parent_primary_image_tag,
		AlbumPrimaryImageTag = parent_primary_image_tag,
		ImageTags = helpers.default_image_tags(),
		ImageBlurHashes = helpers.default_image_blur_hashes(release_cover_info),
		HasLyrics = false,
		NormalizationGain = nil,
		UserData = helpers.build_item_user_data(
			track.db_id,
			track_item_id,
			user_id,
			tostring(track.disc or 0) .. "-" .. tostring(track.track or 0) .. (module.trim(track.track_title) or ""),
			true,
			favorite_lookup,
			context
		),
		MediaSources = media_sources,
		MediaStreams = media_streams,
	}
	return item
end

function helpers.to_release_stub(release: ReleaseLike?): ItemInfo?
	if not release or not release.db_id then
		return nil
	end
	local item: ItemInfo = {
		_type = "release",
		_raw = release,
		Id = module.encode_jellyfin_item_id(release.id),
		Name = module.trim(release.release_title) or "Unknown Album",
		SortName = module.trim(release.sort_title) or module.trim(release.release_title) or "Unknown Album",
		DateCreated = helpers.unix_to_iso8601(release.ctime or release.created_at),
		PremiereDate = helpers.release_date_to_premiere_date(release.release_date),
	}
	return item
end

function helpers.to_track_stub(track: TrackLike?): ItemInfo?
	if not track or not track.db_id then
		return nil
	end
	local item: ItemInfo = {
		_type = "track",
		_raw = track,
		Id = module.encode_jellyfin_item_id(track.id),
		Name = module.trim(track.track_title) or "Unknown Track",
		SortName = module.trim(track.sort_title) or module.trim(track.track_title) or "Unknown Track",
		IndexNumber = track.track,
		ParentIndexNumber = track.disc,
		DateCreated = helpers.unix_to_iso8601(track.ctime or track.created_at),
	}
	return item
end

function helpers.to_artist_stub(artist: ArtistLike?): ItemInfo?
	if not artist or not artist.db_id then
		return nil
	end
	local item: ItemInfo = {
		_type = "artist",
		_raw = artist,
		Id = module.encode_jellyfin_item_id(artist.id),
		Name = module.trim(artist.artist_name) or "Unknown Artist",
		SortName = module.trim(artist.sort_name) or module.trim(artist.artist_name) or "Unknown Artist",
		DateCreated = helpers.unix_to_iso8601(artist.created_at),
	}
	return item
end

function helpers.to_playlist_stub(playlist: PlaylistInfo?, child_count: number?): ItemInfo?
	if not playlist or not playlist.db_id then
		return nil
	end

	local count = if child_count ~= nil then child_count else helpers.playlist_track_count(playlist)
	local playlist_name = module.trim(playlist.name) or "Untitled Playlist"
	local item: ItemInfo = {
		_type = "playlist",
		_raw = playlist,
		_child_count = count,
		Id = module.encode_jellyfin_item_id(playlist.id),
		Name = playlist_name,
		SortName = playlist_name,
		ChildCount = count,
		DateCreated = helpers.unix_to_iso8601(playlist.created_at),
	}
	return item
end

function helpers.append_release_stubs(stubs: { ItemInfo }, release_items: { Release }): ()
	for _, release: Release in release_items do
		local stub = helpers.to_release_stub(release)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_release_record_stubs(stubs: { ItemInfo }, release_items: { ReleaseRecord }): ()
	for _, release: ReleaseRecord in release_items do
		local stub = helpers.to_release_stub(release)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_release_record_map_stubs(stubs: { ItemInfo }, release_items: { [string]: ReleaseRecord }): ()
	for _, release: ReleaseRecord in release_items do
		local stub = helpers.to_release_stub(release)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_artist_stubs(stubs: { ItemInfo }, artist_items: { Artist }): ()
	for _, artist: Artist in artist_items do
		local stub = helpers.to_artist_stub(artist)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_artist_record_map_stubs(stubs: { ItemInfo }, artist_items: { [string]: ArtistRecord }): ()
	for _, artist: ArtistRecord in artist_items do
		local stub = helpers.to_artist_stub(artist)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_track_stubs(stubs: { ItemInfo }, track_items: { Track }): ()
	for _, track: Track in track_items do
		local stub = helpers.to_track_stub(track)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_track_record_map_stubs(stubs: { ItemInfo }, track_items: { [string]: TrackRecord }): ()
	for _, track: TrackRecord in track_items do
		local stub = helpers.to_track_stub(track)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

local RELATION_SORT_TOKENS = {
	albumartist = true,
	artist = true,
}

function helpers.needs_artist_relation_enrichment(options: ItemsQueryOptions): boolean
	if #options.artist_ids > 0 then
		return true
	end

	for _, token: string in options.sort_by do
		if RELATION_SORT_TOKENS[helpers.lower_or_empty(token)] then
			return true
		end
	end

	return false
end

local RUST_SORT_TOKEN_MAP: { [string]: string? } = {
	sortname = "sortname",
	name = "name",
	datecreated = "datecreated",
	premieredate = "releasedate",
	indexnumber = "track",
	parentindexnumber = "disc",
	runtime = "duration",
	id = "id",
}

function helpers.all_sort_tokens_query_eligible(sort_tokens: { string }): boolean
	for _, token: string in sort_tokens do
		if RUST_SORT_TOKEN_MAP[helpers.lower_or_empty(token)] == nil then
			return false
		end
	end
	return true
end

function helpers.rust_sort_tokens(sort_tokens: { string }): { string }
	local mapped: { string } = {}
	for _, token: string in sort_tokens do
		local rust_token = RUST_SORT_TOKEN_MAP[helpers.lower_or_empty(token)]
		if rust_token then
			table.insert(mapped, rust_token)
		end
	end
	return mapped
end

function helpers.needs_full_conversion(options: ItemsQueryOptions): boolean
	return #options.genre_ids > 0 or helpers.needs_artist_relation_enrichment(options)
end

function helpers.can_use_paginated_query(options: ItemsQueryOptions): boolean
	return #options.exclude_item_ids == 0
end

function helpers.sort_tokens_from_options(options: ItemsQueryOptions): { string }
	if #options.sort_by == 0 then
		return { "sortname" }
	end

	local normalized: { string } = {}
	for _, token: string in options.sort_by do
		local normalized_token = helpers.lower_or_empty(token)
		if normalized_token ~= "" then
			table.insert(normalized, normalized_token)
		end
	end

	if #normalized == 0 then
		return { "sortname" }
	end

	return normalized
end

function helpers.sort_value_for_item(item: ItemInfo?, sort_token: string): (SortValueKind, SortValue?)
	if sort_token == "parentindexnumber" then
		return "number", tonumber(if item then item.ParentIndexNumber else nil)
	end
	if sort_token == "indexnumber" then
		return "number", tonumber(if item then item.IndexNumber else nil)
	end
	if sort_token == "name" then
		return "string", helpers.lower_or_empty(if item then item.Name else nil)
	end
	if sort_token == "albumartist" then
		return "string", helpers.lower_or_empty(if item then (item.AlbumArtist or item.Artist or item.Name) else nil)
	end
	if sort_token == "artist" then
		return "string", helpers.lower_or_empty(if item then (item.Artist or item.AlbumArtist or item.Name) else nil)
	end
	if sort_token == "release" then
		return "string", helpers.lower_or_empty(if item then (item.Album or item.Name) else nil)
	end
	if sort_token == "id" then
		return "string", helpers.lower_or_empty(if item then item.Id else nil)
	end
	if sort_token == "runtime" then
		return "number", tonumber(if item then item.RunTimeTicks else nil)
	end
	if sort_token == "datecreated" then
		return "string", if item then item.DateCreated else nil
	end
	if sort_token == "premieredate" then
		return "string", if item then item.PremiereDate else nil
	end
	if sort_token == "random" then
		return "number", 0
	end
	if sort_token == "communityrating" then
		return "number", tonumber(if item then item.CommunityRating else nil)
	end
	if sort_token == "dateplayed" then
		return "string", if item then item.DateCreated else nil
	end
	if sort_token == "playcount" then
		return "number", 0
	end

	return "string", helpers.lower_or_empty(if item then (item.SortName or item.Name) else nil)
end

function helpers.compare_sort_values(
	a_kind: SortValueKind,
	a_value: SortValue?,
	b_kind: SortValueKind,
	b_value: SortValue?,
	descending: boolean
): number
	if a_value == nil and b_value == nil then
		return 0
	end
	if a_value == nil then
		return 1
	end
	if b_value == nil then
		return -1
	end

	if a_kind ~= b_kind then
		a_value = tostring(a_value)
		b_value = tostring(b_value)
	end

	if a_value == b_value then
		return 0
	end

	local comparison: number
	if a_kind == "number" and b_kind == "number" then
		local a_number = tonumber(a_value)
		local b_number = tonumber(b_value)
		if a_number == nil or b_number == nil then
			comparison = 0
		else
			comparison = if a_number < b_number then -1 else 1
		end
	else
		local a_text = tostring(a_value)
		local b_text = tostring(b_value)
		comparison = if a_text < b_text then -1 else 1
	end
	if descending then
		comparison *= -1
	end
	return comparison
end

function helpers.shuffle(items)
	for i = #items, 2, -1 do
		local j = math.random(1, i)
		items[i], items[j] = items[j], items[i]
	end
end

function module.apply_sort(items: { ItemInfo }, options: ItemsQueryOptions)
	local sort_tokens = helpers.sort_tokens_from_options(options)
	local descending = helpers.lower_or_empty(options.sort_order) == "descending"

	if sort_tokens[1] == "random" then
		helpers.shuffle(items)
		return
	end

	local token_count = #sort_tokens

	-- Precompute sort keys to avoid re-extracting on every comparison.
	-- For each item, store kinds[1..token_count+2] and values[1..token_count+2]
	-- where +1 = name tiebreaker, +2 = id tiebreaker.
	local all_kinds: { { SortValueKind } } = {}
	local all_values: { { SortValue? } } = {}
	for i, item: ItemInfo in items do
		local kinds: { SortValueKind } = {}
		local values: { SortValue? } = {}
		for j, sort_token: string in sort_tokens do
			local kind, value = helpers.sort_value_for_item(item, sort_token)
			kinds[j] = kind
			values[j] = value
		end
		local _, name_value = helpers.sort_value_for_item(item, "name")
		local _, id_value = helpers.sort_value_for_item(item, "id")
		kinds[token_count + 1] = "string"
		values[token_count + 1] = name_value
		kinds[token_count + 2] = "string"
		values[token_count + 2] = id_value
		all_kinds[i] = kinds
		all_values[i] = values
	end

	local indices: { number } = {}
	for i = 1, #items do
		indices[i] = i
	end

	table.sort(indices, function(a_idx: number, b_idx: number): boolean
		local a_kinds = all_kinds[a_idx]
		local a_values = all_values[a_idx]
		local b_kinds = all_kinds[b_idx]
		local b_values = all_values[b_idx]

		for j = 1, token_count do
			local cmp = helpers.compare_sort_values(a_kinds[j], a_values[j], b_kinds[j], b_values[j], descending)
			if cmp < 0 then
				return true
			end
			if cmp > 0 then
				return false
			end
		end

		local name_cmp =
			helpers.compare_sort_values("string", a_values[token_count + 1], "string", b_values[token_count + 1], false)
		if name_cmp < 0 then
			return true
		end
		if name_cmp > 0 then
			return false
		end

		return helpers.compare_sort_values(
			"string",
			a_values[token_count + 2],
			"string",
			b_values[token_count + 2],
			false
		) < 0
	end)

	-- Reorder items in-place via sorted indices
	local sorted: { ItemInfo } = {}
	for i, idx: number in indices do
		sorted[i] = items[idx]
	end
	for i = 1, #sorted do
		items[i] = sorted[i]
	end
end

function helpers.apply_ids_filter(items: { ItemInfo }, ids: { string }): { ItemInfo }
	if #ids == 0 then
		return items
	end

	local allowed_ids: { [string]: boolean } = {}
	for _, id_value in ids do
		allowed_ids[id_value] = true
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		local item_id = if item then item.Id else nil
		if item_id and allowed_ids[item_id] then
			table.insert(filtered, item)
		end
	end

	return filtered
end

function helpers.apply_exclude_ids_filter(items: { ItemInfo }, ids: { string }): { ItemInfo }
	if #ids == 0 then
		return items
	end

	local excluded_ids: { [string]: boolean } = {}
	for _, id_value in ids do
		excluded_ids[id_value] = true
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		local item_id = if item then item.Id else nil
		if item_id == nil or not excluded_ids[item_id] then
			table.insert(filtered, item)
		end
	end

	return filtered
end

function helpers.normalize_item_ids(raw_ids: { string }): { string }
	local normalized_ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_id in raw_ids do
		local stripped = string.gsub(raw_id, "-", "")
		local normalized = string.lower(stripped)
		if #normalized == ITEM_ID_HEX_LENGTH and string.match(normalized, ITEM_ID_HEX_PATTERN) then
			if not seen[normalized] then
				seen[normalized] = true
				table.insert(normalized_ids, normalized)
			end
		end
	end

	return normalized_ids
end

function helpers.merge_id_lists(first: { string }, second: { string }): { string }
	local merged: { string } = {}
	local seen: { [string]: boolean } = {}

	for _, id_value in first do
		if not seen[id_value] then
			seen[id_value] = true
			table.insert(merged, id_value)
		end
	end
	for _, id_value in second do
		if not seen[id_value] then
			seen[id_value] = true
			table.insert(merged, id_value)
		end
	end

	return merged
end

function helpers.item_matches_artist_ids(item: ItemInfo?, allowed_artist_ids: { [string]: boolean }): boolean
	if item == nil then
		return false
	end

	local item_type = helpers.lower_or_empty(item.Type)
	if (item_type == "musicartist" or item._type == "artist") and item.Id and allowed_artist_ids[item.Id] then
		return true
	end

	local artist_items = item.ArtistItems
	if artist_items ~= nil then
		for _, artist_item in artist_items do
			local artist_id = if artist_item then artist_item.Id else nil
			if artist_id and allowed_artist_ids[artist_id] then
				return true
			end
		end
	end

	local release_artists = item.AlbumArtists
	if release_artists ~= nil then
		for _, artist_item in release_artists do
			local artist_id = if artist_item then artist_item.Id else nil
			if artist_id and allowed_artist_ids[artist_id] then
				return true
			end
		end
	end

	return false
end

function helpers.apply_artist_ids_filter(items: { ItemInfo }, artist_ids: { string }): { ItemInfo }
	if #artist_ids == 0 then
		return items
	end

	local allowed_artist_ids: { [string]: boolean } = {}
	for _, artist_id in artist_ids do
		allowed_artist_ids[artist_id] = true
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		if helpers.item_matches_artist_ids(item, allowed_artist_ids) then
			table.insert(filtered, item)
		end
	end

	return filtered
end

function helpers.apply_exclude_artist_ids_filter(items: { ItemInfo }, artist_ids: { string }): { ItemInfo }
	if #artist_ids == 0 then
		return items
	end

	local excluded_artist_ids: { [string]: boolean } = {}
	for _, artist_id in artist_ids do
		excluded_artist_ids[artist_id] = true
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		if not helpers.item_matches_artist_ids(item, excluded_artist_ids) then
			table.insert(filtered, item)
		end
	end

	return filtered
end

function helpers.enrich_stubs_with_genre_info(stubs: { ItemInfo })
	local release_ids: { number } = {}
	local release_seen: { [number]: boolean } = {}
	local track_ids: { number } = {}
	local track_to_release: { [number]: number } = {}

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local db_id = if raw then raw.db_id else nil
		if db_id == nil or db_id <= 0 then
			continue
		end

		if stub._type == "release" then
			if not release_seen[db_id] then
				release_seen[db_id] = true
				table.insert(release_ids, db_id)
			end
		elseif stub._type == "track" then
			table.insert(track_ids, db_id)
		end
	end

	if #track_ids > 0 then
		local track_releases_map = releases.list_many(track_ids)
		if type(track_releases_map) == "table" then
			for key, related_releases in track_releases_map do
				local track_id = helpers.resolve_numeric_id(key)
				if track_id and type(related_releases) == "table" and #related_releases > 0 then
					local release_entity: Release = related_releases[1]
					if release_entity.db_id then
						track_to_release[track_id] = release_entity.db_id
						if not release_seen[release_entity.db_id] then
							release_seen[release_entity.db_id] = true
							table.insert(release_ids, release_entity.db_id)
						end
					end
				end
			end
		end
	end

	local genre_items_by_release: { [number]: { ArtistProjectionItem }? } = {}
	local genre_infos_by_release: { [number]: { genres.GenreInfo } } = {}
	if #release_ids > 0 then
		local all_genres = genres.get_for_releases_many(release_ids)
		if type(all_genres) == "table" then
			for key, infos in all_genres do
				local release_id = helpers.resolve_numeric_id(key)
				if release_id and type(infos) == "table" and #infos > 0 then
					genre_items_by_release[release_id] = helpers.genre_items(infos)
					genre_infos_by_release[release_id] = infos
				end
			end
		end
	end

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local db_id = if raw then raw.db_id else nil
		if db_id == nil or db_id <= 0 then
			continue
		end

		local release_db_id: number? = nil
		if stub._type == "release" then
			release_db_id = db_id
		elseif stub._type == "track" then
			release_db_id = track_to_release[db_id]
		end

		if release_db_id then
			stub.GenreItems = genre_items_by_release[release_db_id]
		end
	end

	return genre_infos_by_release
end

function helpers.apply_genre_ids_filter(items: { ItemInfo }, genre_ids: { string }): { ItemInfo }
	if #genre_ids == 0 then
		return items
	end

	local allowed: { [string]: boolean } = {}
	for _, id in genre_ids do
		allowed[id] = true
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		local item_genre_items = if item then item.GenreItems else nil
		if item_genre_items then
			for _, gi in item_genre_items do
				if gi.Id and allowed[gi.Id] then
					table.insert(filtered, item)
					break
				end
			end
		end
	end
	return filtered
end

function helpers.enrich_stubs_with_artist_relations(stubs: { ItemInfo })
	local track_ids: { number } = {}
	local release_ids: { number } = {}
	local track_seen: { [number]: boolean } = {}
	local release_seen: { [number]: boolean } = {}

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local db_id = if raw then raw.db_id else nil
		if db_id and db_id > 0 then
			if stub._type == "track" then
				helpers.append_unique_id(db_id, track_ids, track_seen)
			elseif stub._type == "release" then
				helpers.append_unique_id(db_id, release_ids, release_seen)
			end
		end
	end

	local artist_by_owner: ArtistRelationMap = {}
	local track_release_map: ReleaseRelationMap = {}

	if #track_ids > 0 then
		local track_artists_map = artists_module.list_many(track_ids)
		helpers.merge_artist_relation_map(artist_by_owner, track_artists_map)

		local track_releases_map = releases.list_many(track_ids)
		for key: number, related_releases: { Release } in track_releases_map do
			local track_id = helpers.resolve_numeric_id(key)
			if track_id ~= nil and type(related_releases) == "table" then
				track_release_map[track_id] = related_releases
				for _, release: Release in related_releases do
					helpers.append_unique_id(release.db_id, release_ids, release_seen)
				end
			end
		end
	end

	if #release_ids > 0 then
		local release_artists_map = artists_module.list_many(release_ids)
		helpers.merge_artist_relation_map(artist_by_owner, release_artists_map)
	end

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local db_id = if raw then raw.db_id else nil
		if db_id == nil or db_id <= 0 then
			continue
		end

		if stub._type == "release" then
			local related = artist_by_owner[db_id]
			if related and #related > 0 then
				local primary_name, _, projection_items = helpers.build_artist_projection(related)
				stub.AlbumArtist = primary_name
				stub.AlbumArtists = projection_items
				stub.ArtistItems = projection_items
			end
		elseif stub._type == "track" then
			local track_related = artist_by_owner[db_id]
			if track_related and #track_related > 0 then
				local primary_name, _, projection_items = helpers.build_artist_projection(track_related)
				stub.Artist = primary_name
				stub.ArtistItems = projection_items
			end

			local track_releases = track_release_map[db_id]
			if track_releases and #track_releases > 0 then
				local first_release = track_releases[1]
				local first_release_db_id =
					helpers.resolve_numeric_id(if first_release then first_release.db_id else nil)
				local release_related = if first_release_db_id then artist_by_owner[first_release_db_id] else nil
				if release_related and #release_related > 0 then
					local release_primary_name, _, release_projection_items =
						helpers.build_artist_projection(release_related)
					stub.AlbumArtist = release_primary_name
					stub.AlbumArtists = release_projection_items
				end
			end
		end
	end
end

function helpers.wants_favorite_only(options: ItemsQueryOptions?): boolean
	if options and options.is_favorite == true then
		return true
	end

	local filters = if options then options.filters else nil
	if type(filters) ~= "table" then
		return false
	end

	for _, filter_value in filters do
		if helpers.lower_or_empty(filter_value) == "isfavorite" then
			return true
		end
	end

	return false
end

function helpers.favorite_id_lookup(options: ItemsQueryOptions?, force_load: boolean): FavoriteLookup?
	if options and type(options._favorite_id_lookup) == "table" then
		return options._favorite_id_lookup
	end

	if not force_load and not helpers.wants_favorite_only(options) then
		return nil
	end

	local user_id = if options then options.user_id else nil
	local lookup: FavoriteLookup = {}
	if not user_id or user_id <= 0 then
		if options then
			options._favorite_id_lookup = lookup
		end
		return lookup
	end

	for _, kind in FAVORITE_ENTITY_KINDS do
		local ok, ids = pcall(favorites.list_ids, user_id, kind)
		if ok and type(ids) == "table" then
			for _, target_id in ids do
				if type(target_id) == "number" and target_id > 0 then
					lookup[target_id] = true
				end
			end
		else
			-- `favorites.list_ids` errors rather than silently truncating above the server cap.
			-- Surface as a warning so power users with very large favorite sets see why the
			-- IsFavorite filter returns zero items for that kind.
			print(`favorites.list_ids failed for user_id={user_id} kind={kind}: {tostring(ids)}`)
		end
	end

	if options then
		options._favorite_id_lookup = lookup
	end

	return lookup
end

function helpers.apply_favorite_filter(items: { ItemInfo }, options: ItemsQueryOptions): { ItemInfo }
	if not helpers.wants_favorite_only(options) then
		return items
	end

	local lookup = helpers.favorite_id_lookup(options, true)
	if not lookup then
		return {}
	end

	local filtered: { ItemInfo } = {}
	for _, item in items do
		local user_data = if item then item.UserData else nil
		if type(user_data) == "table" and user_data.IsFavorite == true then
			table.insert(filtered, item)
		else
			local raw = if item then item._raw else nil
			local db_id = if raw then raw.db_id else nil
			if db_id and db_id > 0 and lookup[db_id] then
				table.insert(filtered, item)
			end
		end
	end

	return filtered
end

function helpers.favorite_query_ids(options: ItemsQueryOptions): { string }
	local lookup = helpers.favorite_id_lookup(options, true)
	if not lookup then
		return {}
	end

	local unique_db_ids: { number } = {}
	for db_id in lookup do
		table.insert(unique_db_ids, db_id)
	end
	table.sort(unique_db_ids)

	local hex_map = module.encode_jellyfin_db_ids(unique_db_ids)
	local ids: { string } = {}
	for _, db_id in unique_db_ids do
		local hex = hex_map[db_id]
		if hex then
			table.insert(ids, hex)
		end
	end
	return ids
end

function helpers.query_requests_media_fields(fields: { string }): boolean
	local requested: { [string]: boolean } = {}
	for _, field in fields do
		local normalized = helpers.lower_or_empty(field)
		if normalized ~= "" then
			requested[normalized] = true
		end
	end

	return requested["mediasources"] == true or requested["mediastreams"] == true
end

function helpers.query_requests_people(fields: { string }): boolean
	for _, field in fields do
		if helpers.lower_or_empty(field) == "people" then
			return true
		end
	end
	return false
end

function module.coerce_items_query(query: ApiQueryParams?): ItemsQueryOptions
	local options: ItemsQueryOptions = {
		include_item_types = {},
		ids = {},
		exclude_item_ids = {},
		exclude_artist_ids = {},
		primary_artist_ids = {},
		contributing_artist_ids = {},
		track_artist_ids = {},
		release_artist_ids = {},
		album_ids = {},
		artist_ids = {},
		genre_ids = {},
		filters = {},
		is_favorite = nil,
		parent_id = nil,
		start_index = 0,
		limit = nil,
		recursive = nil,
		sort_by = {},
		sort_order = nil,
		fields = {},
		search_term = nil,
		enable_user_data = nil,
		enable_images = nil,
		include_media_info = false,
		include_people = false,
		group_items = true,
		user_id = nil,
		_favorite_id_lookup = nil,
	}
	options.include_item_types = helpers.parse_query_csv(query, { "IncludeItemTypes", "includeItemTypes" })
	options.ids = helpers.parse_query_csv(query, { "ids", "Ids" })
	options.exclude_item_ids = helpers.normalize_item_ids(
		helpers.parse_query_csv(query, { "ExcludeItemIds", "excludeItemIds", "excludeitemids" })
	)
	options.exclude_artist_ids = helpers.normalize_item_ids(
		helpers.parse_query_csv(query, { "ExcludeArtistIds", "excludeArtistIds", "excludeartistids" })
	)
	local artist_ids =
		helpers.normalize_item_ids(helpers.parse_query_csv(query, { "ArtistIds", "artistIds", "artistids" }))
	local release_artist_ids = helpers.normalize_item_ids(
		helpers.parse_query_csv(query, { "AlbumArtistIds", "albumArtistIds", "albumartistids" })
	)
	local contributing_artist_ids = helpers.normalize_item_ids(
		helpers.parse_query_csv(query, { "ContributingArtistIds", "contributingArtistIds", "contributingartistids" })
	)
	options.primary_artist_ids = artist_ids
	options.contributing_artist_ids = contributing_artist_ids
	options.track_artist_ids = helpers.merge_id_lists(artist_ids, contributing_artist_ids)
	options.release_artist_ids = release_artist_ids
	options.album_ids =
		helpers.normalize_item_ids(helpers.parse_query_csv(query, { "AlbumIds", "albumIds", "albumids" }))
	options.artist_ids =
		helpers.merge_id_lists(helpers.merge_id_lists(artist_ids, release_artist_ids), contributing_artist_ids)
	options.genre_ids =
		helpers.normalize_item_ids(helpers.parse_query_csv(query, { "GenreIds", "genreIds", "genreids" }))
	options.filters = helpers.parse_query_csv(query, { "Filters", "filters" })
	options.is_favorite = helpers.parse_query_bool(query, { "IsFavorite", "isFavorite" }, false)
	options.parent_id = helpers.parse_query_id(query, { "ParentId", "parentId" })
	options.start_index = module.parse_query_int(query, { "StartIndex", "startIndex" }, 0, 0, nil) or 0
	options.limit = module.parse_query_int(query, { "Limit", "limit" }, nil, 0, nil)
	options.recursive = helpers.parse_query_bool(query, { "Recursive", "recursive" }, false)
	options.sort_by = helpers.parse_query_csv(query, { "SortBy", "sortBy" })
	options.sort_order = module.parse_query_string(query, { "SortOrder", "sortOrder" }, "Ascending")
	options.fields = helpers.parse_query_csv(query, { "Fields", "fields" })
	options.search_term = module.parse_query_string(query, { "SearchTerm", "searchTerm" }, nil)
	options.enable_user_data = helpers.parse_query_bool(query, { "EnableUserData", "enableUserData" }, true)
	options.enable_images = helpers.parse_query_bool(query, { "EnableImages", "enableImages" }, true)
	if #options.fields == 0 then
		options.fields = DEFAULT_ITEMS_FIELDS
	end
	options.include_media_info = helpers.query_requests_media_fields(options.fields)
	options.include_people = helpers.query_requests_people(options.fields)
	options.group_items = helpers.parse_query_bool(query, { "GroupItems", "groupItems" }, true) ~= false
	return options
end

function module.with_user_query_options(options: ItemsQueryOptions, user_id: number): ItemsQueryOptions
	local scoped_options = helpers.clone_items_query_options(options)
	scoped_options.user_id = user_id
	return scoped_options
end

function helpers.wants_item_type(options: ItemsQueryOptions, item_type: string): boolean
	if #options.include_item_types == 0 then
		return true
	end

	for _, include_item_type in options.include_item_types do
		if helpers.lower_or_empty(include_item_type) == item_type then
			return true
		end
	end

	return false
end

function helpers.item_type_selection(options: ItemsQueryOptions): ItemTypeSelection
	return {
		constrained = #options.include_item_types > 0,
		releases = helpers.wants_item_type(options, "musicalbum"),
		artists = helpers.wants_item_type(options, "musicartist"),
		tracks = helpers.wants_item_type(options, "audio"),
		playlists = helpers.wants_item_type(options, "playlist"),
		genres = helpers.wants_item_type(options, "musicgenre"),
		libraries = helpers.wants_item_type(options, "collectionfolder"),
	}
end

function helpers.selected_single_entity_type(selection: ItemTypeSelection): ItemKind?
	if not selection.constrained then
		return nil
	end

	local count = 0
	if selection.releases then
		count += 1
	end
	if selection.artists then
		count += 1
	end
	if selection.tracks then
		count += 1
	end
	if selection.playlists then
		count += 1
	end

	if count ~= 1 then
		return nil
	end

	if selection.releases then
		return "release" :: ItemKind
	end
	if selection.artists then
		return "artist" :: ItemKind
	end
	if selection.tracks then
		return "track" :: ItemKind
	end
	if selection.playlists then
		return "playlist" :: ItemKind
	end
	return nil
end

function helpers.new_realization_context(options: ItemsQueryOptions): ItemContext
	local favorite_lookup: FavoriteLookup = {}
	if options.enable_user_data ~= false then
		favorite_lookup = helpers.favorite_id_lookup(options, true) or {}
	end

	return {
		user_id = options.user_id,
		favorite_lookup = favorite_lookup,
		play_count_lookup = {},
		last_played_lookup = {},
		include_user_data = options.enable_user_data ~= false,
		include_images = options.enable_images ~= false,
		include_people = options.include_people == true,
		related_artists_by_owner_id = {},
		credited_artists_by_owner_id = {},
		release_tracks_by_release_id = {},
		track_releases_by_track_id = {},
		artist_tracks_by_artist_id = {},
		playback_source_by_track_id = {},
		container_by_track_id = {},
		has_source_prefetch = false,
		has_container_prefetch = false,
		tag_infos_by_item_id = options._tag_infos_by_item_id,
		library_hex_id = helpers.resolve_library_hex_id_from_parent(options.parent_id),
	}
end

function helpers.collect_realization_seed(stubs: { ItemInfo }, context: ItemContext): RealizationSeedInfo
	local seed: RealizationSeedInfo = {
		track_ids = {},
		release_ids = {},
		artist_ids = {},
		playlist_db_ids = {},
		track_seen = {},
		release_seen = {},
		artist_seen = {},
		has_track_stubs = false,
		has_release_stubs = false,
		has_artist_stubs = false,
	}

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local db_id = if raw then raw.db_id else nil
		local resolved_id = helpers.resolve_numeric_id(db_id)
		if resolved_id ~= nil then
			context.play_count_lookup[resolved_id] = 0
		end

		if stub._type == "track" then
			seed.has_track_stubs = true
			helpers.append_unique_id(db_id, seed.track_ids, seed.track_seen)
		elseif stub._type == "release" then
			seed.has_release_stubs = true
			helpers.append_unique_id(db_id, seed.release_ids, seed.release_seen)
		elseif stub._type == "artist" then
			seed.has_artist_stubs = true
			helpers.append_unique_id(db_id, seed.artist_ids, seed.artist_seen)
		elseif stub._type == "playlist" and resolved_id then
			table.insert(seed.playlist_db_ids, resolved_id)
		end
	end

	return seed
end

function helpers.prefetch_library_hex_ids(context: ItemContext, seed: RealizationSeedInfo): ()
	if context.library_hex_id then
		return
	end

	local all_entity_ids: { number } = {}
	for _, id in seed.release_ids do
		table.insert(all_entity_ids, id)
	end
	for _, id in seed.artist_ids do
		table.insert(all_entity_ids, id)
	end
	if #all_entity_ids == 0 then
		return
	end

	local ok, lib_map = pcall(libraries.get_for_entities, all_entity_ids)
	if ok and lib_map then
		local hex_map: { [number]: string } = {}
		for entity_id, lib in lib_map do
			local hex = base.nanoid_to_hex(lib.id)
			if hex then
				hex_map[entity_id] = hex
			end
		end
		context.library_hex_id_by_entity_id = hex_map
	end
end

function helpers.prefetch_track_artists(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_track_stubs or #seed.track_ids == 0 then
		return
	end

	helpers.ensure_relation_entries(context.related_artists_by_owner_id, seed.track_ids)
	helpers.ensure_relation_entries(context.track_releases_by_track_id, seed.track_ids)

	local track_artists_map = artists_module.list_many(seed.track_ids)
	helpers.merge_artist_relation_map(context.related_artists_by_owner_id, track_artists_map)
	context.has_track_artists_prefetch = helpers.relation_map_has_entries(track_artists_map)
end

function helpers.prefetch_track_releases(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_track_stubs or #seed.track_ids == 0 then
		return
	end

	local track_releases_map = releases.list_many(seed.track_ids)
	helpers.merge_release_relation_map(context.track_releases_by_track_id, track_releases_map)
	context.has_track_releases_prefetch = helpers.relation_map_has_entries(track_releases_map)
	for _, related_releases: { Release } in track_releases_map do
		for _, release: Release in related_releases do
			helpers.append_unique_id(release.db_id, seed.release_ids, seed.release_seen)
		end
	end
	context.track_release_lookup_complete = true
end

function helpers.prefetch_release_covers(context: ItemContext, seed: RealizationSeedInfo): ()
	if
		context.include_images ~= true
		or (not seed.has_release_stubs and not seed.has_track_stubs)
		or #seed.release_ids == 0
	then
		return
	end

	local cover_release_ids: { number } = {}
	for _, release_id: number in seed.release_ids do
		table.insert(cover_release_ids, release_id)
	end
	local ok, raw_covers = pcall(covers.get_many, cover_release_ids)
	if ok and type(raw_covers) == "table" then
		local cover_lookup: { [number]: base.ResolvedCoverInfo } = {}
		local prefetch_set: { [number]: boolean } = {}
		for _, release_id: number in cover_release_ids do
			prefetch_set[release_id] = true
			local raw = raw_covers[release_id]
			if type(raw) == "table" then
				local info = base.normalize_cover_info(raw)
				if info then
					cover_lookup[release_id] = info
				end
			end
		end
		context.cover_info_by_release_id = cover_lookup
		context.cover_prefetch_release_ids = prefetch_set
	end
end

function helpers.prefetch_release_artists_and_genres(
	context: ItemContext,
	seed: RealizationSeedInfo,
	options: ItemsQueryOptions
): ()
	if (not seed.has_release_stubs and not seed.has_track_stubs) or #seed.release_ids == 0 then
		return
	end

	local release_artists_map = artists_module.list_many(seed.release_ids)
	helpers.merge_artist_relation_map(context.related_artists_by_owner_id, release_artists_map)
	context.has_release_artists_prefetch = helpers.relation_map_has_entries(release_artists_map)

	local genre_cache: { [number]: { genres.GenreInfo } } = {}
	if type(options._genre_infos_by_release_id) == "table" then
		genre_cache = options._genre_infos_by_release_id
	else
		local all_genres = genres.get_for_releases_many(seed.release_ids)
		if type(all_genres) == "table" then
			for key, infos in all_genres do
				local release_id = helpers.resolve_numeric_id(key)
				if release_id and type(infos) == "table" and #infos > 0 then
					genre_cache[release_id] = infos
				end
			end
		end
	end
	context.genre_infos_by_release_id = genre_cache
end

function helpers.prefetch_credit_relations(context: ItemContext, seed: RealizationSeedInfo): ()
	if context.include_people ~= true or (not seed.has_release_stubs and not seed.has_track_stubs) then
		return
	end

	local ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, release_id in seed.release_ids do
		helpers.append_unique_id(release_id, ids, seen)
	end
	for _, track_id in seed.track_ids do
		helpers.append_unique_id(track_id, ids, seen)
	end
	if #ids == 0 then
		return
	end

	helpers.ensure_relation_entries(context.credited_artists_by_owner_id, ids)
	local projection_lookup = helpers.query_many_projections(ids, { "credits" })
	for _, id in ids do
		local credits = helpers.projection_credits(projection_lookup[tostring(id)])
		if credits ~= nil then
			context.credited_artists_by_owner_id[id] = credits
		end
	end
end

function helpers.prefetch_release_tracks(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_release_stubs or #seed.release_ids == 0 then
		return
	end

	local release_tracks_map = tracks.list_many(seed.release_ids)
	helpers.merge_track_relation_map(context.release_tracks_by_release_id, release_tracks_map)
	context.has_release_tracks_prefetch = helpers.relation_map_has_entries(release_tracks_map)
end

function helpers.prefetch_artist_tracks(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_artist_stubs or #seed.artist_ids == 0 then
		return
	end

	local artist_tracks_map = tracks.list_many(seed.artist_ids)
	helpers.merge_track_relation_map(context.artist_tracks_by_artist_id, artist_tracks_map)
	context.has_artist_tracks_prefetch = helpers.relation_map_has_entries(artist_tracks_map)
end

function helpers.prefetch_track_stats_and_media(
	context: ItemContext,
	seed: RealizationSeedInfo,
	options: ItemsQueryOptions
): ()
	if #seed.track_ids == 0 then
		return
	end

	if context.include_user_data == true then
		local stats = listens.get_stats(seed.track_ids, options.user_id)
		if type(stats) == "table" then
			local play_counts = stats.counts
			if type(play_counts) == "table" then
				for key, value in play_counts do
					local resolved_key = helpers.resolve_numeric_id(key)
					if resolved_key ~= nil then
						context.play_count_lookup[resolved_key] = tonumber(value) or 0
					end
				end
			end
			local last_played = stats.last_played
			if type(last_played) == "table" then
				for key, value in last_played do
					local resolved_key = helpers.resolve_numeric_id(key)
					if resolved_key ~= nil then
						context.last_played_lookup[resolved_key] = tonumber(value) or 0
					end
				end
			end
		end
	end

	if options.include_media_info == true then
		local sources = playback_sources.get_many(seed.track_ids, true)
		if type(sources) == "table" then
			context.playback_source_by_track_id = sources
			context.has_source_prefetch = true
		end
	else
		local containers = track_sources.get_primary_containers(seed.track_ids)
		if type(containers) == "table" then
			context.container_by_track_id = containers
			context.has_container_prefetch = true
		end
	end
end

function helpers.prefetch_playlist_track_ids(context: ItemContext, seed: RealizationSeedInfo): ()
	if #seed.playlist_db_ids == 0 then
		return
	end

	local tracks_map = playlists.get_tracks_many(seed.playlist_db_ids)
	if type(tracks_map) == "table" then
		local track_ids_map: { [number]: { number } } = {}
		for pl_id, track_links in tracks_map do
			local ids: { number } = {}
			if type(track_links) == "table" then
				for _, link in track_links do
					if type(link) == "table" and link.track_id and link.track_id > 0 then
						table.insert(ids, link.track_id)
					end
				end
			end
			track_ids_map[pl_id] = ids
		end
		context.playlist_track_ids_by_playlist_id = track_ids_map
	end
end

function helpers.collect_tag_target_ids(seed: RealizationSeedInfo): { number }
	local target_ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, id: number in seed.track_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: number in seed.release_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: number in seed.artist_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: number in seed.playlist_db_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	return target_ids
end

function helpers.prefetch_item_tags(context: ItemContext, seed: RealizationSeedInfo, options: ItemsQueryOptions): ()
	local target_ids = helpers.collect_tag_target_ids(seed)
	if #target_ids == 0 then
		return
	end

	if type(options._tag_infos_by_item_id) == "table" then
		for _, target_id in target_ids do
			if options._tag_infos_by_item_id[target_id] == nil then
				options._tag_infos_by_item_id[target_id] = {}
			end
		end
		context.tag_infos_by_item_id = options._tag_infos_by_item_id
		return
	end

	context.tag_infos_by_item_id = helpers.fetch_tag_info_map(options.user_id, target_ids)
end

function helpers.build_realization_context(stubs: { ItemInfo }, options: ItemsQueryOptions): ItemContext
	local context = helpers.new_realization_context(options)
	local seed = helpers.collect_realization_seed(stubs, context)

	helpers.prefetch_library_hex_ids(context, seed)
	helpers.prefetch_track_artists(context, seed)
	helpers.prefetch_track_releases(context, seed)
	helpers.prefetch_release_covers(context, seed)
	helpers.prefetch_release_artists_and_genres(context, seed, options)
	helpers.prefetch_credit_relations(context, seed)
	helpers.prefetch_release_tracks(context, seed)
	helpers.prefetch_artist_tracks(context, seed)
	helpers.prefetch_track_stats_and_media(context, seed, options)
	helpers.prefetch_playlist_track_ids(context, seed)
	helpers.prefetch_item_tags(context, seed, options)

	return context
end

function helpers.realize_stubs(stubs: { ItemInfo }, options: ItemsQueryOptions): { ItemInfo }
	local items: { ItemInfo } = {}
	local user_id = options.user_id
	local context = helpers.build_realization_context(stubs, options)
	local favorite_lookup = context.favorite_lookup
	local parent_scope = helpers.resolve_parent_scope(options.parent_id)
	local scoped_release_id = if parent_scope.entity_type == "Release" then parent_scope.db_id else nil
	for _, stub: ItemInfo in stubs do
		local item: ItemInfo? = nil
		local raw = stub._raw
		if stub._type == "release" then
			item = helpers.to_release_item(helpers.release_like_from_raw(raw), user_id, favorite_lookup, context)
		elseif stub._type == "artist" then
			item = helpers.to_artist_item(helpers.artist_like_from_raw(raw), user_id, favorite_lookup, context)
		elseif stub._type == "track" then
			item = helpers.to_track_item(
				helpers.track_like_from_raw(raw),
				scoped_release_id,
				user_id,
				favorite_lookup,
				options.include_media_info,
				context
			)
		elseif stub._type == "playlist" then
			item = helpers.to_playlist_item(helpers.playlist_info_from_raw(raw), user_id, stub._child_count, context)
		else
			item = stub
		end
		if item then
			table.insert(items, item)
		end
	end
	return items
end

function helpers.page_and_realize_stubs(stubs: { ItemInfo }, options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local paged_stubs, total_count, start_index = module.paginate_items(stubs, options.start_index, options.limit)
	local items = helpers.realize_stubs(paged_stubs, options)
	return items, total_count, start_index
end
function module.list_playlist_track_items(
	playlist: PlaylistInfo?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number)
	if not playlist or not playlist.db_id then
		local empty_items: { ItemInfo } = {}
		return empty_items, 0, options.start_index or 0
	end

	if #options.include_item_types > 0 and not helpers.wants_item_type(options, "audio") then
		local empty_items: { ItemInfo } = {}
		return empty_items, 0, options.start_index or 0
	end

	local stubs = {}
	local playlist_item_ids = {}
	local track_refs = module.playlist_track_refs(playlist)

	local track_ids_to_fetch: { number } = {}
	for _, track_ref in track_refs do
		local track_id = if track_ref
				and track_ref.track_id ~= nil
				and track_ref.track_id > 0
			then track_ref.track_id
			else nil
		if track_id ~= nil then
			table.insert(track_ids_to_fetch, track_id)
		end
	end

	local tracks_by_id: { [number]: Track? } = if #track_ids_to_fetch > 0
		then tracks.get_by_ids(track_ids_to_fetch)
		else {}

	for _, track_ref in track_refs do
		local track_id = if track_ref
				and track_ref.track_id ~= nil
				and track_ref.track_id > 0
			then track_ref.track_id
			else nil
		if track_id ~= nil then
			local track = tracks_by_id[track_id]
			if track then
				local stub = helpers.to_track_stub(track)
				if stub then
					table.insert(stubs, stub)
					table.insert(
						playlist_item_ids,
						module.encode_jellyfin_item_id(if track_ref then track_ref.entry_id else nil)
					)
				end
			end
		end
	end

	local items = helpers.realize_stubs(stubs, options)
	for index, item in items do
		item.PlaylistItemId = playlist_item_ids[index]
	end

	items = module.apply_search_filter(items, options.search_term)
	items = helpers.apply_ids_filter(items, options.ids)
	items = helpers.apply_exclude_ids_filter(items, options.exclude_item_ids)
	items = helpers.apply_favorite_filter(items, options)
	items = helpers.apply_artist_ids_filter(items, options.artist_ids)
	items = helpers.apply_genre_ids_filter(items, options.genre_ids)
	if #options.sort_by > 0 then
		module.apply_sort(items, options)
	end

	local paged_items, total_count, start_index = module.paginate_items(items, options.start_index, options.limit)
	return paged_items, total_count, start_index
end

function helpers.item_list_result(items: { ItemInfo }, total_count: number, start_index: number): ItemListResult
	return {
		items = items,
		total_count = total_count,
		start_index = start_index,
	}
end

function helpers.empty_item_list_result(start_index: number): ItemListResult
	local empty_items: { ItemInfo } = {}
	return helpers.item_list_result(empty_items, 0, start_index)
end

function helpers.track_items_by_scope(scope: number?): { Track }
	if scope == nil then
		return helpers.call_module_get(tracks.list)
	end
	if helpers.is_library_scope(scope) then
		local ok, result = pcall(tracks.list_by_library, scope :: number)
		return if ok and type(result) == "table" then result else {}
	end
	return helpers.call_module_get(tracks.list, scope)
end

function helpers.artist_items_by_scope(scope: number?): { Artist }
	if scope == nil then
		return helpers.call_module_get(artists_module.list)
	end
	if helpers.is_library_scope(scope) then
		local ok, result = pcall(artists_module.list_by_library, scope :: number)
		return if ok and type(result) == "table" then result else {}
	end
	return helpers.call_module_get(artists_module.list, scope)
end

function helpers.direct_track_items_by_release(release_id: number): { Track }
	local ok, result = pcall(tracks.list_many, { release_id })
	if not ok or type(result) ~= "table" then
		return {}
	end

	local release_tracks = result[release_id]
	return if type(release_tracks) == "table" then release_tracks else {}
end

function helpers.list_release_child_items(options: ItemsQueryOptions, release_id: number): ItemListResult
	if not helpers.wants_item_type(options, "audio") then
		return helpers.empty_item_list_result(options.start_index or 0)
	end

	local stubs: { ItemInfo } = {}
	helpers.append_track_stubs(stubs, helpers.direct_track_items_by_release(release_id))

	local needs_genre_filter = #options.genre_ids > 0
	if needs_genre_filter then
		local genre_infos = helpers.enrich_stubs_with_genre_info(stubs)
		options._genre_infos_by_release_id = genre_infos
	end

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	if needs_genre_filter then
		stubs = helpers.apply_genre_ids_filter(stubs, options.genre_ids)
	end

	if #options.artist_ids > 0 then
		helpers.enrich_stubs_with_artist_relations(stubs)
		stubs = helpers.apply_artist_ids_filter(stubs, options.artist_ids) :: { ItemInfo }
	end
	module.apply_sort(stubs, options)

	local items, total_count, start_index = helpers.page_and_realize_stubs(stubs, options)
	return helpers.item_list_result(items, total_count, start_index)
end

function helpers.try_list_jellyfin_parent_children(
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo
): ItemListResult?
	if parent_scope.db_id == nil then
		return nil
	end

	if parent_scope.entity_type == "Release" then
		return helpers.list_release_child_items(options, parent_scope.db_id)
	end

	if parent_scope.entity_type == "Artist" then
		local artist_releases = releases.get_by_artist(parent_scope.db_id)
		if artist_releases == nil then
			return nil
		end

		local stubs: { ItemInfo } = {}
		helpers.append_release_stubs(stubs, artist_releases)
		module.apply_sort(stubs, options)
		local items, total_count, start_index = helpers.page_and_realize_stubs(stubs, options)
		return { items = items, total_count = total_count, start_index = start_index }
	end

	return nil
end

function helpers.append_scoped_listing_stubs(
	stubs: { ItemInfo },
	selected_types: ItemTypeSelection,
	scope: number?,
	user_id: number?
): ()
	if selected_types.releases then
		helpers.append_release_stubs(stubs, helpers.call_module_get(releases.list, scope))
	end

	if selected_types.artists then
		helpers.append_artist_stubs(stubs, helpers.artist_items_by_scope(scope))
	end

	if selected_types.tracks then
		helpers.append_track_stubs(stubs, helpers.track_items_by_scope(scope))
	end

	if selected_types.playlists and scope == nil then
		local playlist_items = helpers.list_user_playlists(user_id)
		local viewable_playlists: { PlaylistInfo } = {}
		for _, playlist: PlaylistInfo in playlist_items do
			if module.playlist_can_view(playlist, user_id) then
				table.insert(viewable_playlists, playlist)
			end
		end

		local playlist_db_ids: { number } = {}
		for _, playlist: PlaylistInfo in viewable_playlists do
			if playlist.db_id and playlist.db_id > 0 then
				table.insert(playlist_db_ids, playlist.db_id)
			end
		end
		local tracks_by_playlist = if #playlist_db_ids > 0 then playlists.get_tracks_many(playlist_db_ids) else {}

		for _, playlist: PlaylistInfo in viewable_playlists do
			local track_links = if playlist.db_id then tracks_by_playlist[playlist.db_id] else nil
			local count = if track_links then #track_links else 0
			local stub = helpers.to_playlist_stub(playlist, count)
			if stub then
				table.insert(stubs, stub)
			end
		end
	end
end

function helpers.list_items_by_ids(options: ItemsQueryOptions)
	local selected_types = helpers.item_type_selection(options)

	local decoded_ids: { number } = {}
	local decoded_seen: { [number]: boolean } = {}
	local decoded_by_raw: { [string]: number } = {}
	for _, raw_id: string in options.ids do
		local decoded_id = helpers.decode_jellyfin_item_id(raw_id)
		if decoded_id ~= nil then
			decoded_by_raw[raw_id] = decoded_id
			if not decoded_seen[decoded_id] then
				decoded_seen[decoded_id] = true
				table.insert(decoded_ids, decoded_id)
			end
		end
	end
	local projection_lookup = helpers.query_many_projections(decoded_ids, nil)

	local stubs: { ItemInfo } = {}
	for _, raw_id: string in options.ids do
		local db_id = decoded_by_raw[raw_id]
		if db_id then
			local stub: ItemInfo? = nil
			local projection = projection_lookup[tostring(db_id)]
			if projection == nil then
				local request: EntityQueryRequest = { id = db_id }
				local ok, value = pcall(entities.query, request)
				if ok and type(value) == "table" then
					projection = value :: EntityProjectionInfo
				end
			end
			if projection ~= nil then
				local entity_type = projection.entity_type

				if entity_type == "release" and selected_types.releases then
					stub = helpers.to_release_stub(helpers.release_record_from_projection(projection))
				elseif entity_type == "artist" and selected_types.artists then
					stub = helpers.to_artist_stub(helpers.artist_record_from_projection(projection))
				elseif entity_type == "track" and selected_types.tracks then
					stub = helpers.to_track_stub(helpers.track_record_from_projection(projection))
				end
			end

			if not stub and selected_types.playlists then
				local playlist = module.find_playlist_by_id(db_id)
				if playlist and module.playlist_can_view(playlist, options.user_id) then
					stub = helpers.to_playlist_stub(playlist)
				end
			end

			if not stub and selected_types.genres then
				local ok, genre_info = pcall(genres.get_by_id, db_id)
				if ok and genre_info then
					stub = helpers.to_genre_item(genre_info.name, genre_info.id, nil, nil, nil, nil)
				end
			end

			if not stub and selected_types.libraries then
				stub = helpers.find_library_view_item_by_id(db_id)
			end

			if stub then
				table.insert(stubs, stub)
			end
		end
	end

	local needs_genre_filter = #options.genre_ids > 0
	if needs_genre_filter then
		local genre_infos = helpers.enrich_stubs_with_genre_info(stubs)
		options._genre_infos_by_release_id = genre_infos
	end

	-- Non-artist filters first to reduce stubs before artist enrichment.
	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_favorite_filter(stubs, options)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	if needs_genre_filter then
		stubs = helpers.apply_genre_ids_filter(stubs, options.genre_ids)
	end

	if helpers.needs_artist_relation_enrichment(options) then
		helpers.enrich_stubs_with_artist_relations(stubs)
		stubs = helpers.apply_artist_ids_filter(stubs, options.artist_ids)
	end
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function helpers.list_items_by_album_ids(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local decoded_album_ids = helpers.decode_item_filter_ids(options.album_ids)
	if #decoded_album_ids == 0 then
		local empty_items: { ItemInfo } = {}
		return empty_items, 0, options.start_index or 0
	end
	local selected_types = helpers.item_type_selection(options)

	local projection_lookup = helpers.query_many_projections(decoded_album_ids, nil)
	local release_records: { ReleaseRecord } = {}
	local release_ids: { number } = {}
	local release_seen: { [number]: boolean } = {}

	for _, album_id: number in decoded_album_ids do
		local projection = projection_lookup[tostring(album_id)]
		if projection == nil then
			local request: EntityQueryRequest = { id = album_id }
			local ok, value = pcall(entities.query, request)
			if ok and type(value) == "table" then
				projection = value :: EntityProjectionInfo
			end
		end

		local release_record = helpers.release_record_from_projection(projection)
		local release_db_id = if release_record then helpers.resolve_numeric_id(release_record.db_id) else nil
		if release_record ~= nil and release_db_id ~= nil and not release_seen[release_db_id] then
			release_seen[release_db_id] = true
			table.insert(release_ids, release_db_id)
			table.insert(release_records, release_record)
		end
	end

	local stubs: { ItemInfo } = {}
	if selected_types.releases then
		helpers.append_release_record_stubs(stubs, release_records)
	end

	if selected_types.tracks and #release_ids > 0 then
		local tracks_by_release = tracks.list_many(release_ids)
		for _, release_id: number in release_ids do
			local release_tracks = tracks_by_release[release_id]
			if type(release_tracks) == "table" then
				helpers.append_track_stubs(stubs, release_tracks)
			end
		end
	end

	local needs_genre_filter = #options.genre_ids > 0
	if needs_genre_filter then
		local genre_infos = helpers.enrich_stubs_with_genre_info(stubs)
		options._genre_infos_by_release_id = genre_infos
	end

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	if needs_genre_filter then
		stubs = helpers.apply_genre_ids_filter(stubs, options.genre_ids)
	end

	if helpers.needs_artist_relation_enrichment(options) then
		helpers.enrich_stubs_with_artist_relations(stubs)
		stubs = helpers.apply_artist_ids_filter(stubs, options.artist_ids) :: { ItemInfo }
	end
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function helpers.collect_scoped_item_ids<T>(raw_items: { T }, get_id: (T) -> string?): { [string]: boolean }
	local ids: { [string]: boolean } = {}
	for _, raw_item: T in raw_items do
		local item_id = module.encode_jellyfin_item_id(get_id(raw_item))
		if item_id then
			ids[item_id] = true
		end
	end

	return ids
end

function helpers.resolve_single_entity_type(options: ItemsQueryOptions): ItemKind?
	return helpers.selected_single_entity_type(helpers.item_type_selection(options))
end

function helpers.list_items_by_artist_ids(
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo
): ({ ItemInfo }?, number?, number?)
	local scope = parent_scope.db_id
	local decoded_primary_artist_ids = helpers.decode_artist_filter_ids(options.primary_artist_ids or {})
	local decoded_contributing_artist_ids = helpers.decode_artist_filter_ids(options.contributing_artist_ids or {})
	local decoded_explicit_track_artist_ids = helpers.decode_artist_filter_ids(options.track_artist_ids or {})
	local decoded_explicit_release_artist_ids = helpers.decode_artist_filter_ids(options.release_artist_ids or {})
	local decoded_track_artist_ids =
		helpers.merge_numeric_id_lists(decoded_primary_artist_ids, decoded_contributing_artist_ids)
	if #decoded_track_artist_ids == 0 then
		decoded_track_artist_ids = decoded_explicit_track_artist_ids
	end
	local decoded_release_artist_ids =
		helpers.merge_numeric_id_lists(decoded_primary_artist_ids, decoded_explicit_release_artist_ids)
	local decoded_artist_ids = helpers.merge_numeric_id_lists(
		helpers.merge_numeric_id_lists(decoded_track_artist_ids, decoded_release_artist_ids),
		helpers.decode_artist_filter_ids(options.artist_ids or {})
	)
	if #decoded_artist_ids == 0 then
		return nil, nil, nil
	end

	local sort_tokens = helpers.sort_tokens_from_options(options)
	local is_random = sort_tokens[1] == "random"
	local entity_type = helpers.resolve_single_entity_type(options)
	local has_primary_artist_filter = #decoded_primary_artist_ids > 0
	local has_track_artist_filter = #decoded_track_artist_ids > 0
	local has_release_artist_filter = #decoded_release_artist_ids > 0
	local is_contributing_only = #decoded_contributing_artist_ids > 0
		and #decoded_primary_artist_ids == 0
		and #decoded_explicit_release_artist_ids == 0
	local track_query_allowed = entity_type == "track" and (has_track_artist_filter or has_release_artist_filter)
	local allow_sort_fallback = entity_type == "track" and track_query_allowed
	local release_query_allowed = entity_type == "release" and not has_primary_artist_filter and not is_contributing_only
	if
		#options.ids == 0
		and not is_random
		and (helpers.all_sort_tokens_query_eligible(sort_tokens) or allow_sort_fallback)
		and not helpers.wants_favorite_only(options)
		and helpers.can_use_paginated_query(options)
		and (release_query_allowed or track_query_allowed)
	then
		if entity_type == "track" then
			local query_opts: TrackQueryOptions = {
				scope = scope,
				artist_ids = if has_track_artist_filter then decoded_track_artist_ids else nil,
				release_artist_ids = if has_release_artist_filter then decoded_release_artist_ids else nil,
				sort_by = helpers.rust_sort_tokens(sort_tokens),
				sort_order = helpers.normalize_sort_order(options.sort_order),
				offset = options.start_index,
				limit = options.limit,
				search_term = options.search_term,
			}
			local ok, result = pcall(tracks.query, query_opts)
			if ok and type(result) == "table" then
				local stubs: { ItemInfo } = {}
				helpers.append_track_stubs(stubs, result.entities)

				local items = helpers.realize_stubs(stubs, options)
				return items, result.total_count, result.offset
			end
		elseif entity_type == "release" then
			local query_opts: ReleaseQueryOptions = {
				scope = scope,
				artist_ids = decoded_artist_ids,
				sort_by = helpers.rust_sort_tokens(sort_tokens),
				sort_order = helpers.normalize_sort_order(options.sort_order),
				offset = options.start_index,
				limit = options.limit,
				search_term = options.search_term,
			}
			local ok, result = pcall(releases.query, query_opts)
			if ok and type(result) == "table" then
				local stubs: { ItemInfo } = {}
				helpers.append_release_stubs(stubs, result.entities)

				local items = helpers.realize_stubs(stubs, options)
				return items, result.total_count, result.offset
			end
		end
	end

	local wants_releases = helpers.wants_item_type(options, "musicalbum")
	local wants_artists = helpers.wants_item_type(options, "musicartist")
	local wants_tracks = helpers.wants_item_type(options, "audio")
	local needs_release_artist_tracks = wants_tracks and #decoded_release_artist_ids > 0

	local allowed_release_ids: { [string]: boolean }? = nil
	local allowed_artist_ids: { [string]: boolean }? = nil
	local allowed_track_ids: { [string]: boolean }? = nil
	if scope ~= nil then
		if wants_releases then
			allowed_release_ids = helpers.collect_scoped_item_ids(
				helpers.call_module_get(releases.list, scope),
				function(release: Release): string?
					return release.id
				end
			)
		end
		if wants_artists then
			allowed_artist_ids = helpers.collect_scoped_item_ids(
				helpers.artist_items_by_scope(scope),
				function(artist: Artist): string?
					return artist.id
				end
			)
		end
		if wants_tracks then
			allowed_track_ids = helpers.collect_scoped_item_ids(
				helpers.track_items_by_scope(scope),
				function(track: Track): string?
					return track.id
				end
			)
		end
	end

	local include_values: { string } = {}
	if (wants_releases and not is_contributing_only) or needs_release_artist_tracks then
		table.insert(include_values, "releases")
	end
	if wants_tracks and #decoded_track_artist_ids > 0 then
		table.insert(include_values, "tracks")
	end

	local releases_by_id: { [string]: ReleaseRecord } = {}
	local artists_by_id: { [string]: ArtistRecord } = {}
	local tracks_by_id: { [string]: TrackRecord } = {}
	local release_ids_for_track_lookup: { number } = {}
	local release_ids_for_track_lookup_seen: { [number]: boolean } = {}
	local query_failed = false

	local projection_lookup =
		helpers.query_many_projections(decoded_artist_ids, if #include_values > 0 then include_values else nil)

	for _, artist_id: number in decoded_artist_ids do
		local projection = projection_lookup[tostring(artist_id)]
		if projection == nil then
			local request: EntityQueryRequest = { id = artist_id }
			if #include_values > 0 then
				request.include = include_values
			end
			local ok, value = pcall(entities.query, request)
			if ok and type(value) == "table" then
				projection = value :: EntityProjectionInfo
			end
		end

		if projection == nil or projection.entity_type ~= "artist" then
			query_failed = true
			break
		end

		local artist = helpers.artist_record_from_projection(projection)
		local artist_item_id = module.encode_jellyfin_item_id(if artist then artist.id else nil)
		if
			artist
			and wants_artists
			and artist_item_id
			and (allowed_artist_ids == nil or allowed_artist_ids[artist_item_id])
		then
			artists_by_id[artist_item_id] = artist
		end

		local includes = projection.includes
		if wants_releases or needs_release_artist_tracks then
			local included_releases = helpers.release_records_from_releases(includes.releases)
			if included_releases ~= nil then
				for _, release: ReleaseRecord in included_releases do
					local release_item_id = module.encode_jellyfin_item_id(release.id)
					local release_db_id = helpers.resolve_numeric_id(release.db_id)
					local release_allowed = (allowed_release_ids == nil)
						or (release_item_id and allowed_release_ids[release_item_id])
					if wants_releases and release_item_id and release_allowed then
						releases_by_id[release_item_id] = release
					end
					if needs_release_artist_tracks and release_db_id ~= nil and release_allowed then
						if not release_ids_for_track_lookup_seen[release_db_id] then
							release_ids_for_track_lookup_seen[release_db_id] = true
							table.insert(release_ids_for_track_lookup, release_db_id)
						end
					end
				end
			end
		end

		if wants_tracks and #decoded_track_artist_ids > 0 then
			local included_tracks = helpers.track_records_from_tracks(includes.tracks)
			if included_tracks ~= nil then
				for _, track: TrackRecord in included_tracks do
					local track_item_id = module.encode_jellyfin_item_id(track.id)
					if track_item_id and (allowed_track_ids == nil or allowed_track_ids[track_item_id]) then
						tracks_by_id[track_item_id] = track
					end
				end
			end
		end
	end

	if query_failed then
		return nil, nil, nil
	end

	if wants_releases and has_primary_artist_filter then
		for _, artist_id: number in decoded_primary_artist_ids do
			local ok, appearance_releases = pcall(releases.get_appearances, artist_id)
			if ok and type(appearance_releases) == "table" then
				for _, release: Release in appearance_releases do
					local release_item_id = module.encode_jellyfin_item_id(release.id)
					if
						release_item_id
						and releases_by_id[release_item_id] == nil
						and (allowed_release_ids == nil or allowed_release_ids[release_item_id])
					then
						releases_by_id[release_item_id] = release
					end
				end
			end
		end
	end

	if wants_releases and is_contributing_only then
		for _, artist_id: number in decoded_contributing_artist_ids do
			local ok, appearance_releases = pcall(releases.get_appearances, artist_id)
			if ok and type(appearance_releases) == "table" then
				for _, release: Release in appearance_releases do
					local release_item_id = module.encode_jellyfin_item_id(release.id)
					if
						release_item_id
						and releases_by_id[release_item_id] == nil
						and (allowed_release_ids == nil or allowed_release_ids[release_item_id])
					then
						releases_by_id[release_item_id] = release
					end
				end
			end
		end
	end

	if needs_release_artist_tracks and #release_ids_for_track_lookup > 0 then
		local release_tracks_map = tracks.list_many(release_ids_for_track_lookup)
		for _, release_tracks: { Track } in release_tracks_map do
			for _, track: Track in release_tracks do
				local track_item_id = module.encode_jellyfin_item_id(track.id)
				if track_item_id and (allowed_track_ids == nil or allowed_track_ids[track_item_id]) then
					tracks_by_id[track_item_id] = track
				end
			end
		end
	end

	local stubs: { ItemInfo } = {}
	if wants_releases then
		helpers.append_release_record_map_stubs(stubs, releases_by_id)
	end

	if wants_artists then
		helpers.append_artist_record_map_stubs(stubs, artists_by_id)
	end

	if wants_tracks then
		helpers.append_track_record_map_stubs(stubs, tracks_by_id)
	end

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function helpers.list_items_full(options: ItemsQueryOptions, parent_scope: ParentScopeInfo)
	local scope = parent_scope.db_id
	local user_id = options.user_id
	local selected_types = helpers.item_type_selection(options)
	local stubs: { ItemInfo } = {}

	helpers.append_scoped_listing_stubs(stubs, selected_types, scope, user_id)

	if #options.genre_ids > 0 then
		local genre_infos = helpers.enrich_stubs_with_genre_info(stubs)
		options._genre_infos_by_release_id = genre_infos
	end

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	stubs = helpers.apply_genre_ids_filter(stubs, options.genre_ids)

	if helpers.needs_artist_relation_enrichment(options) then
		helpers.enrich_stubs_with_artist_relations(stubs)
		stubs = helpers.apply_artist_ids_filter(stubs, options.artist_ids) :: { ItemInfo }
	end
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function helpers.query_single_type_with_sort(
	entity_type: ItemKind,
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo,
	sort_by: { string },
	sort_order: "ascending" | "descending"
): ({ ItemInfo }?, number?, number?)
	local scope = parent_scope.db_id

	if entity_type == "release" then
		local query_opts: ReleaseQueryOptions = {
			scope = scope,
			artist_ids = nil,
			sort_by = sort_by,
			sort_order = sort_order,
			offset = options.start_index,
			limit = options.limit,
			search_term = options.search_term,
		}
		local ok, result = pcall(releases.query, query_opts)
		if not ok or type(result) ~= "table" then
			return nil, nil, nil
		end

		local stubs: { ItemInfo } = {}
		helpers.append_release_stubs(stubs, result.entities)

		local items = helpers.realize_stubs(stubs, options)
		return items, result.total_count, result.offset
	elseif entity_type == "artist" then
		local query_opts: ArtistQueryOptions = {
			scope = scope,
			sort_by = sort_by,
			sort_order = sort_order,
			offset = options.start_index,
			limit = options.limit,
			search_term = options.search_term,
		}
		local ok, result = pcall(artists_module.query, query_opts)
		if not ok or type(result) ~= "table" then
			return nil, nil, nil
		end

		local stubs: { ItemInfo } = {}
		helpers.append_artist_stubs(stubs, result.entities)

		local items = helpers.realize_stubs(stubs, options)
		return items, result.total_count, result.offset
	elseif entity_type == "track" then
		local query_opts: TrackQueryOptions = {
			scope = scope,
			artist_ids = nil,
			release_artist_ids = nil,
			sort_by = sort_by,
			sort_order = sort_order,
			offset = options.start_index,
			limit = options.limit,
			search_term = options.search_term,
		}
		local ok, result = pcall(tracks.query, query_opts)
		if not ok or type(result) ~= "table" then
			return nil, nil, nil
		end

		local stubs: { ItemInfo } = {}
		helpers.append_track_stubs(stubs, result.entities)

		local items = helpers.realize_stubs(stubs, options)
		return items, result.total_count, result.offset
	end

	return nil, nil, nil
end

function helpers.try_query_single_type_items(
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo
): ({ ItemInfo }?, number?, number?)
	local sort_tokens = helpers.sort_tokens_from_options(options)
	local is_random = sort_tokens[1] == "random"
	if
		is_random
		or helpers.wants_favorite_only(options)
		or not helpers.all_sort_tokens_query_eligible(sort_tokens)
		or not helpers.can_use_paginated_query(options)
	then
		return nil, nil, nil
	end

	local entity_type = helpers.resolve_single_entity_type(options)
	if not entity_type then
		return nil, nil, nil
	end

	return helpers.query_single_type_with_sort(
		entity_type,
		options,
		parent_scope,
		helpers.rust_sort_tokens(sort_tokens),
		helpers.normalize_sort_order(options.sort_order)
	)
end

function helpers.list_scoped_items_fallback(
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo
): ({ ItemInfo }, number, number)
	local scope = parent_scope.db_id
	local selected_types = helpers.item_type_selection(options)
	local stubs: { ItemInfo } = {}
	helpers.append_scoped_listing_stubs(stubs, selected_types, scope, options.user_id)

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function helpers.try_list_direct_item_filters(options: ItemsQueryOptions): ItemListResult?
	if #options.album_ids > 0 then
		local items, total_count, start_index = helpers.list_items_by_album_ids(options)
		return helpers.item_list_result(items, total_count, start_index)
	end

	if #options.ids > 0 then
		local items, total_count, start_index = helpers.list_items_by_ids(options)
		return helpers.item_list_result(items, total_count, start_index)
	end

	return nil
end

function helpers.try_list_root_favorite_items(
	options: ItemsQueryOptions,
	parent_scope: ParentScopeInfo
): ItemListResult?
	if not helpers.wants_favorite_only(options) or parent_scope.db_id ~= nil then
		return nil
	end

	local favorite_ids = helpers.favorite_query_ids(options)
	local favorite_options = helpers.clone_items_query_options(options)
	favorite_options.ids = favorite_ids
	favorite_options._favorite_id_lookup = options._favorite_id_lookup
	local items, total_count, start_index = helpers.list_items_by_ids(favorite_options)
	return helpers.item_list_result(items, total_count, start_index)
end

function module.list_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local direct_items = helpers.try_list_direct_item_filters(options)
	if direct_items then
		return direct_items.items, direct_items.total_count, direct_items.start_index
	end

	local parent_scope = helpers.resolve_parent_scope(options.parent_id)
	local parent_children = helpers.try_list_jellyfin_parent_children(options, parent_scope)
	if parent_children then
		return parent_children.items, parent_children.total_count, parent_children.start_index
	end

	local favorite_items = helpers.try_list_root_favorite_items(options, parent_scope)
	if favorite_items then
		return favorite_items.items, favorite_items.total_count, favorite_items.start_index
	end

	if #options.artist_ids > 0 and #options.genre_ids == 0 then
		local items, total_count, offset = helpers.list_items_by_artist_ids(options, parent_scope)
		if items and total_count ~= nil and offset ~= nil then
			return items, total_count, offset
		end
	end

	if helpers.needs_full_conversion(options) then
		return helpers.list_items_full(options, parent_scope)
	end

	local items, total_count, offset = helpers.try_query_single_type_items(options, parent_scope)
	if items and total_count ~= nil and offset ~= nil then
		return items, total_count, offset
	end

	return helpers.list_scoped_items_fallback(options, parent_scope)
end

function helpers.group_latest_items_by_album(stubs: { ItemInfo }, limit: number?): { ItemInfo }
	local track_ids: { number } = {}
	for _, stub in stubs do
		if stub._type == "track" and stub._raw and stub._raw.db_id then
			table.insert(track_ids, stub._raw.db_id)
		end
	end

	local track_releases_map: { [number]: { Release } } = {}
	if #track_ids > 0 then
		track_releases_map = releases.list_many(track_ids) or {}
	end

	local result: { ItemInfo } = {}
	local seen_release_ids: { [number]: number } = {}
	local effective_limit = limit or 50

	for _, stub in stubs do
		if #result >= effective_limit then
			break
		end

		if stub._type ~= "track" then
			continue
		end

		local track = stub._raw
		local track_db_id = if track then track.db_id else nil
		if not track_db_id then
			table.insert(result, stub)
			continue
		end

		local track_releases = track_releases_map[track_db_id]
		if not track_releases or #track_releases == 0 then
			table.insert(result, stub)
			continue
		end

		local release = track_releases[1]
		local release_db_id = if release then release.db_id else nil
		if not release_db_id then
			table.insert(result, stub)
			continue
		end

		local existing_idx = seen_release_ids[release_db_id]
		if existing_idx then
			local existing = result[existing_idx]
			if existing and existing._child_count then
				existing._child_count = existing._child_count + 1
			end
		else
			local release_stub = helpers.to_release_stub(release)
			if release_stub then
				release_stub._child_count = 1
				table.insert(result, release_stub)
				seen_release_ids[release_db_id] = #result
			else
				table.insert(result, stub)
			end
		end
	end

	return result
end

function helpers.apply_latest_sort(items: { ItemInfo }): ()
	-- TODO: Use a true "date added" field once the model exposes one; db_id ordering is our current proxy.
	table.sort(items, function(a, b)
		local a_id_value = tonumber(if a then a.Id else nil)
		local b_id_value = tonumber(if b then b.Id else nil)

		if a_id_value and b_id_value and a_id_value ~= b_id_value then
			return a_id_value > b_id_value
		end
		if a_id_value and not b_id_value then
			return true
		end
		if b_id_value and not a_id_value then
			return false
		end

		local a_name = helpers.lower_or_empty(if a then a.Name else nil)
		local b_name = helpers.lower_or_empty(if b then b.Name else nil)
		if a_name == b_name then
			return helpers.lower_or_empty(if a then a.SortName else nil)
				< helpers.lower_or_empty(if b then b.SortName else nil)
		end
		return a_name < b_name
	end)
end

function module.list_latest_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local parent_scope = helpers.resolve_parent_scope(options.parent_id)
	local scope = parent_scope.db_id
	local selected_types = helpers.item_type_selection(options)

	if #options.artist_ids > 0 then
		local stubs: { ItemInfo } = {}
		helpers.append_scoped_listing_stubs(stubs, selected_types, scope, options.user_id)

		if #options.genre_ids > 0 then
			local genre_infos = helpers.enrich_stubs_with_genre_info(stubs)
			options._genre_infos_by_release_id = genre_infos
		end
		stubs = module.apply_search_filter(stubs, options.search_term)
		stubs = helpers.apply_ids_filter(stubs, options.ids)
		stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
		stubs = helpers.apply_favorite_filter(stubs, options)
		stubs = helpers.apply_genre_ids_filter(stubs, options.genre_ids)
		if #options.artist_ids > 0 then
			helpers.enrich_stubs_with_artist_relations(stubs)
			stubs = helpers.apply_artist_ids_filter(stubs, options.artist_ids) :: { ItemInfo }
		end
		helpers.apply_latest_sort(stubs)
		if options.group_items then
			stubs = helpers.group_latest_items_by_album(stubs, options.limit)
		end

		return helpers.page_and_realize_stubs(stubs, options)
	end

	local entity_type = helpers.resolve_single_entity_type(options)
	local use_paginated_query = entity_type
		and not helpers.wants_favorite_only(options)
		and helpers.can_use_paginated_query(options)
		and not options.group_items
	if use_paginated_query and entity_type then
		local items, total_count, offset =
			helpers.query_single_type_with_sort(entity_type, options, parent_scope, { "id" }, "descending")
		if items and total_count ~= nil and offset ~= nil then
			return items, total_count, offset
		end
	end

	local stubs: { ItemInfo } = {}
	helpers.append_scoped_listing_stubs(stubs, selected_types, scope, options.user_id)

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	helpers.apply_latest_sort(stubs)
	if options.group_items then
		stubs = helpers.group_latest_items_by_album(stubs, options.limit)
	end

	return helpers.page_and_realize_stubs(stubs, options)
end

local similar = {}

function similar.genre_lookup_from_infos(infos: { genres.GenreInfo }?): { [string]: boolean }
	local lookup: { [string]: boolean } = {}
	if type(infos) ~= "table" then
		return lookup
	end

	for _, info in infos do
		local name = module.trim(info.name)
		if name then
			lookup[string.lower(name)] = true
		end
	end

	return lookup
end

function similar.has_genre_lookup_values(lookup: { [string]: boolean }): boolean
	for _ in lookup do
		return true
	end
	return false
end

function similar.genre_infos_overlap(
	target_lookup: { [string]: boolean },
	requires_overlap: boolean,
	infos: { genres.GenreInfo }?
): boolean
	if not requires_overlap then
		return true
	end
	if type(infos) ~= "table" then
		return false
	end

	for _, info in infos do
		local name = module.trim(info.name)
		if name and target_lookup[string.lower(name)] then
			return true
		end
	end

	return false
end

function similar.tag_lookup_from_infos(infos: { TagInfo }?): { [string]: boolean }
	local lookup: { [string]: boolean } = {}
	if type(infos) ~= "table" then
		return lookup
	end

	for _, info in infos do
		local name = module.trim(info.tag)
		if name then
			lookup[string.lower(name)] = true
		end
	end

	return lookup
end

function similar.has_tag_lookup_values(lookup: { [string]: boolean }): boolean
	for _ in lookup do
		return true
	end
	return false
end

function similar.tag_infos_overlap(
	target_lookup: { [string]: boolean },
	requires_overlap: boolean,
	infos: { TagInfo }?
): boolean
	if not requires_overlap then
		return true
	end
	if type(infos) ~= "table" then
		return false
	end

	for _, info in infos do
		local name = module.trim(info.tag)
		if name and target_lookup[string.lower(name)] then
			return true
		end
	end

	return false
end

function similar.release_genre_map(release_ids: { number }): { [number]: { genres.GenreInfo } }
	if #release_ids == 0 then
		return {}
	end

	local ok, raw_genres = pcall(genres.get_for_releases_many, release_ids)
	if not ok or type(raw_genres) ~= "table" then
		return {}
	end

	local mapped: { [number]: { genres.GenreInfo } } = {}
	for key, infos in raw_genres :: { [number]: { genres.GenreInfo } } do
		local release_id = helpers.resolve_numeric_id(key)
		if release_id and type(infos) == "table" and #infos > 0 then
			mapped[release_id] = infos
		end
	end
	return mapped
end

function similar.tag_map(user_id: number?, target_ids: { number }): TagInfoMap
	return helpers.fetch_tag_info_map(user_id, target_ids)
end

function similar.genre_lookup_for_release_ids(
	release_ids: { number },
	genres_by_release_id: { [number]: { genres.GenreInfo } }
): { [string]: boolean }
	local lookup: { [string]: boolean } = {}
	for _, release_id in release_ids do
		for name in similar.genre_lookup_from_infos(genres_by_release_id[release_id]) do
			lookup[name] = true
		end
	end
	return lookup
end

function similar.release_ids_match_genres(
	release_ids: { number }?,
	genres_by_release_id: { [number]: { genres.GenreInfo } },
	target_lookup: { [string]: boolean },
	requires_overlap: boolean
): boolean
	if not requires_overlap then
		return true
	end
	if type(release_ids) ~= "table" then
		return false
	end

	for _, release_id in release_ids do
		if similar.genre_infos_overlap(target_lookup, true, genres_by_release_id[release_id]) then
			return true
		end
	end
	return false
end

function similar.collect_release_ids_from_items(release_items: { Release }): { number }
	local release_ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, release in release_items do
		helpers.append_unique_id(release.db_id, release_ids, seen)
	end
	return release_ids
end

function similar.collect_track_ids_from_items(track_items: { Track }): { number }
	local track_ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, track in track_items do
		helpers.append_unique_id(track.db_id, track_ids, seen)
	end
	return track_ids
end

function similar.collect_release_ids_for_track(track_id: number): { number }
	local release_ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, release in helpers.call_module_get(releases.list, track_id) do
		helpers.append_unique_id(release.db_id, release_ids, seen)
	end
	return release_ids
end

function similar.collect_track_release_ids(track_items: { Track }): ({ [number]: { number } }, { number })
	local track_ids: { number } = {}
	local track_seen: { [number]: boolean } = {}
	for _, track in track_items do
		helpers.append_unique_id(track.db_id, track_ids, track_seen)
	end

	local release_ids_by_track: { [number]: { number } } = {}
	local all_release_ids: { number } = {}
	local all_release_seen: { [number]: boolean } = {}
	if #track_ids == 0 then
		return release_ids_by_track, all_release_ids
	end

	local ok, raw_releases_by_track = pcall(releases.list_many, track_ids)
	if not ok or type(raw_releases_by_track) ~= "table" then
		return release_ids_by_track, all_release_ids
	end

	for key, related_releases in raw_releases_by_track :: { [number]: { Release } } do
		local track_id = helpers.resolve_numeric_id(key)
		if not track_id or type(related_releases) ~= "table" then
			continue
		end

		local track_release_ids: { number } = {}
		local track_release_seen: { [number]: boolean } = {}
		for _, release in related_releases do
			local release_id = helpers.append_unique_id(release.db_id, track_release_ids, track_release_seen)
			helpers.append_unique_id(release_id, all_release_ids, all_release_seen)
		end
		release_ids_by_track[track_id] = track_release_ids
	end

	return release_ids_by_track, all_release_ids
end

function similar.append_artist_release_id(
	release_ids_by_artist: { [number]: { number } },
	release_seen_by_artist: { [number]: { [number]: boolean } },
	artist_id: number?,
	release_id: number?,
	all_release_ids: { number },
	all_release_seen: { [number]: boolean }
): ()
	local resolved_artist_id = helpers.resolve_numeric_id(artist_id)
	local resolved_release_id = helpers.resolve_numeric_id(release_id)
	if not resolved_artist_id or not resolved_release_id then
		return
	end

	if release_ids_by_artist[resolved_artist_id] == nil then
		release_ids_by_artist[resolved_artist_id] = {}
	end
	if release_seen_by_artist[resolved_artist_id] == nil then
		release_seen_by_artist[resolved_artist_id] = {}
	end

	if not release_seen_by_artist[resolved_artist_id][resolved_release_id] then
		release_seen_by_artist[resolved_artist_id][resolved_release_id] = true
		table.insert(release_ids_by_artist[resolved_artist_id], resolved_release_id)
	end
	helpers.append_unique_id(resolved_release_id, all_release_ids, all_release_seen)
end

function similar.collect_artist_release_ids(artist_ids: { number }): ({ [number]: { number } }, { number })
	local release_ids_by_artist: { [number]: { number } } = {}
	local release_seen_by_artist: { [number]: { [number]: boolean } } = {}
	local all_release_ids: { number } = {}
	local all_release_seen: { [number]: boolean } = {}
	if #artist_ids == 0 then
		return release_ids_by_artist, all_release_ids
	end

	for _, artist_id in artist_ids do
		release_ids_by_artist[artist_id] = {}
		release_seen_by_artist[artist_id] = {}
	end

	local ok_tracks, raw_tracks_by_artist = pcall(tracks.list_many, artist_ids)
	if ok_tracks and type(raw_tracks_by_artist) == "table" then
		local track_ids: { number } = {}
		local track_seen: { [number]: boolean } = {}
		local artist_ids_by_track: { [number]: { number } } = {}

		for key, artist_tracks in raw_tracks_by_artist :: { [number]: { Track } } do
			local artist_id = helpers.resolve_numeric_id(key)
			if not artist_id or type(artist_tracks) ~= "table" then
				continue
			end

			for _, track in artist_tracks do
				local track_id = helpers.append_unique_id(track.db_id, track_ids, track_seen)
				if track_id then
					if artist_ids_by_track[track_id] == nil then
						artist_ids_by_track[track_id] = {}
					end
					table.insert(artist_ids_by_track[track_id], artist_id)
				end
			end
		end

		local ok_releases, raw_releases_by_track = pcall(releases.list_many, track_ids)
		if ok_releases and type(raw_releases_by_track) == "table" then
			for key, related_releases in raw_releases_by_track :: { [number]: { Release } } do
				local track_id = helpers.resolve_numeric_id(key)
				local owner_artist_ids = if track_id then artist_ids_by_track[track_id] else nil
				if not owner_artist_ids or type(related_releases) ~= "table" then
					continue
				end

				for _, artist_id in owner_artist_ids do
					for _, release in related_releases do
						similar.append_artist_release_id(
							release_ids_by_artist,
							release_seen_by_artist,
							artist_id,
							release.db_id,
							all_release_ids,
							all_release_seen
						)
					end
				end
			end
		end
	end

	for _, artist_id in artist_ids do
		local ok, appearance_releases = pcall(releases.get_appearances, artist_id)
		if ok and type(appearance_releases) == "table" then
			for _, release in appearance_releases :: { Release } do
				similar.append_artist_release_id(
					release_ids_by_artist,
					release_seen_by_artist,
					artist_id,
					release.db_id,
					all_release_ids,
					all_release_seen
				)
			end
		end
	end

	return release_ids_by_artist, all_release_ids
end

function similar.prepare_similar_options(options: ItemsQueryOptions, item_id: number): ItemsQueryOptions
	local similar_options = helpers.clone_items_query_options(options)
	similar_options.start_index = 0
	local current_hex_id = module.encode_jellyfin_item_id(item_id)
	if current_hex_id then
		similar_options.exclude_item_ids = helpers.merge_id_lists(similar_options.exclude_item_ids, { current_hex_id })
	end
	return similar_options
end

function similar.finish_similar_stubs(stubs: { ItemInfo }, options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	if #options.exclude_artist_ids > 0 then
		helpers.enrich_stubs_with_artist_relations(stubs)
		stubs = helpers.apply_exclude_artist_ids_filter(stubs, options.exclude_artist_ids)
	end
	helpers.shuffle(stubs)
	return helpers.page_and_realize_stubs(stubs, options)
end

function similar.list_similar_release_items(item_id: number, options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local target_lookup = similar.genre_lookup_from_infos(helpers.resolve_release_genres(item_id))
	local requires_overlap = similar.has_genre_lookup_values(target_lookup)
	local target_tag_lookup = similar.tag_lookup_from_infos(helpers.fetch_tag_infos(options.user_id, item_id))
	local requires_tag_overlap = similar.has_tag_lookup_values(target_tag_lookup)
	local release_items = helpers.call_module_get(releases.list)
	local release_ids = similar.collect_release_ids_from_items(release_items)
	local genres_by_release_id = similar.release_genre_map(release_ids)
	local tags_by_release_id = similar.tag_map(options.user_id, release_ids)
	options._genre_infos_by_release_id = genres_by_release_id
	options._tag_infos_by_item_id = tags_by_release_id

	local stubs: { ItemInfo } = {}
	for _, release in release_items do
		local release_id = helpers.resolve_numeric_id(release.db_id)
		if
			release_id
			and release_id ~= item_id
			and similar.genre_infos_overlap(target_lookup, requires_overlap, genres_by_release_id[release_id])
			and similar.tag_infos_overlap(target_tag_lookup, requires_tag_overlap, tags_by_release_id[release_id])
		then
			local stub = helpers.to_release_stub(release)
			if stub then
				table.insert(stubs, stub)
			end
		end
	end

	return similar.finish_similar_stubs(stubs, options)
end

function similar.list_similar_track_items(item_id: number, options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local target_release_ids = similar.collect_release_ids_for_track(item_id)
	local target_genres_by_release_id = similar.release_genre_map(target_release_ids)
	local target_lookup = similar.genre_lookup_for_release_ids(target_release_ids, target_genres_by_release_id)
	local requires_overlap = similar.has_genre_lookup_values(target_lookup)
	local target_tag_lookup = similar.tag_lookup_from_infos(helpers.fetch_tag_infos(options.user_id, item_id))
	local requires_tag_overlap = similar.has_tag_lookup_values(target_tag_lookup)

	local track_items = helpers.call_module_get(tracks.list)
	local release_ids_by_track, all_release_ids = similar.collect_track_release_ids(track_items)
	local genres_by_release_id = similar.release_genre_map(all_release_ids)
	local tags_by_track_id = similar.tag_map(options.user_id, similar.collect_track_ids_from_items(track_items))
	options._genre_infos_by_release_id = genres_by_release_id
	options._tag_infos_by_item_id = tags_by_track_id

	local stubs: { ItemInfo } = {}
	for _, track in track_items do
		local track_id = helpers.resolve_numeric_id(track.db_id)
		if
			track_id
			and track_id ~= item_id
			and similar.release_ids_match_genres(
				release_ids_by_track[track_id],
				genres_by_release_id,
				target_lookup,
				requires_overlap
			)
			and similar.tag_infos_overlap(target_tag_lookup, requires_tag_overlap, tags_by_track_id[track_id])
		then
			local stub = helpers.to_track_stub(track)
			if stub then
				table.insert(stubs, stub)
			end
		end
	end

	return similar.finish_similar_stubs(stubs, options)
end

function similar.list_similar_artist_items(item_id: number, options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local artist_items = helpers.call_module_get(artists_module.list)
	local artist_ids: { number } = { item_id }
	local artist_seen: { [number]: boolean } = { [item_id] = true }
	for _, artist in artist_items do
		helpers.append_unique_id(artist.db_id, artist_ids, artist_seen)
	end

	local release_ids_by_artist, all_release_ids = similar.collect_artist_release_ids(artist_ids)
	local genres_by_release_id = similar.release_genre_map(all_release_ids)
	local target_lookup =
		similar.genre_lookup_for_release_ids(release_ids_by_artist[item_id] or {}, genres_by_release_id)
	local requires_overlap = similar.has_genre_lookup_values(target_lookup)
	local tags_by_artist_id = similar.tag_map(options.user_id, artist_ids)
	local target_tag_lookup = similar.tag_lookup_from_infos(tags_by_artist_id[item_id])
	local requires_tag_overlap = similar.has_tag_lookup_values(target_tag_lookup)
	options._tag_infos_by_item_id = tags_by_artist_id

	local stubs: { ItemInfo } = {}
	for _, artist in artist_items do
		local artist_id = helpers.resolve_numeric_id(artist.db_id)
		if
			artist_id
			and artist_id ~= item_id
			and similar.release_ids_match_genres(
				release_ids_by_artist[artist_id],
				genres_by_release_id,
				target_lookup,
				requires_overlap
			)
			and similar.tag_infos_overlap(target_tag_lookup, requires_tag_overlap, tags_by_artist_id[artist_id])
		then
			local stub = helpers.to_artist_stub(artist)
			if stub then
				table.insert(stubs, stub)
			end
		end
	end

	return similar.finish_similar_stubs(stubs, options)
end

function module.list_similar_items(
	item_id: number?,
	options: ItemsQueryOptions
): ({ ItemInfo }, number, number, boolean)
	local empty_items: { ItemInfo } = {}
	local start_index = 0
	if not item_id or item_id <= 0 then
		return empty_items, 0, start_index, false
	end

	local target = module.find_item_by_id(item_id, options.user_id)
	if not target then
		return empty_items, 0, start_index, false
	end

	local similar_options = similar.prepare_similar_options(options, item_id)
	local item_type = helpers.lower_or_empty(target.Type)
	if item_type == "musicalbum" then
		local items, total_count, result_start_index = similar.list_similar_release_items(item_id, similar_options)
		return items, total_count, result_start_index, true
	elseif item_type == "audio" then
		local items, total_count, result_start_index = similar.list_similar_track_items(item_id, similar_options)
		return items, total_count, result_start_index, true
	elseif item_type == "musicartist" then
		local items, total_count, result_start_index = similar.list_similar_artist_items(item_id, similar_options)
		return items, total_count, result_start_index, true
	end

	return empty_items, 0, start_index, true
end

function module.find_item_by_id(item_id: number?, user_id: number?): ItemInfo?
	if not item_id or item_id <= 0 then
		return nil
	end

	local encoded_item_id = module.encode_jellyfin_item_id(item_id)
	if not encoded_item_id then
		return nil
	end

	local options: ItemsQueryOptions = {
		include_item_types = {},
		ids = { encoded_item_id },
		exclude_item_ids = {},
		exclude_artist_ids = {},
		parent_id = nil,
		start_index = 0,
		limit = 1,
		recursive = true,
		sort_by = {},
		sort_order = "Ascending",
		fields = DEFAULT_ITEMS_FIELDS,
		include_media_info = true,
		include_people = true,
		search_term = nil,
		artist_ids = {},
		genre_ids = {},
		primary_artist_ids = {},
		contributing_artist_ids = {},
		track_artist_ids = {},
		release_artist_ids = {},
		album_ids = {},
		filters = {},
		is_favorite = nil,
		enable_user_data = true,
		enable_images = true,
		group_items = false,
		_favorite_id_lookup = nil,
		user_id = user_id,
	}
	local items = module.list_items(options)
	return helpers.first_or_nil(items)
end

function module.list_artist_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local parent_scope = helpers.resolve_parent_scope(options.parent_id)
	local scope = parent_scope.db_id
	local sort_tokens = helpers.sort_tokens_from_options(options)
	if
		sort_tokens[1] ~= "random"
		and helpers.all_sort_tokens_query_eligible(sort_tokens)
		and not helpers.wants_favorite_only(options)
		and helpers.can_use_paginated_query(options)
	then
		local sort_order = helpers.normalize_sort_order(options.sort_order)
		local query_opts: ArtistQueryOptions = {
			scope = scope,
			sort_by = helpers.rust_sort_tokens(sort_tokens),
			sort_order = sort_order,
			offset = options.start_index,
			limit = options.limit,
			search_term = options.search_term,
		}

		local ok, result = pcall(artists_module.query, query_opts)
		if ok and type(result) == "table" then
			local raw_items = result.entities
			local total_count = result.total_count
			local offset = result.offset

			local stubs: { ItemInfo } = {}
			helpers.append_artist_stubs(stubs, raw_items)

			local items = helpers.realize_stubs(stubs, options)
			return items, total_count, offset
		end
	end

	-- Fallback: build artist stubs, then realize only paged results.
	local stubs = {}
	helpers.append_artist_stubs(stubs, helpers.artist_items_by_scope(scope))

	stubs = module.apply_search_filter(stubs, options.search_term)
	stubs = helpers.apply_ids_filter(stubs, options.ids)
	stubs = helpers.apply_exclude_ids_filter(stubs, options.exclude_item_ids)
	stubs = helpers.apply_favorite_filter(stubs, options)
	module.apply_sort(stubs, options)

	return helpers.page_and_realize_stubs(stubs, options)
end

function module.list_genre_items(options: ItemsQueryOptions): ({ ItemInfo }, number, number)
	local parent_scope = helpers.resolve_parent_scope(options.parent_id)
	local scope = parent_scope.db_id
	local unique: { [string]: { name: string, id: string, db_id: number? } } = {}
	local runtime_ticks_by_genre: { [string]: number } = {}
	local release_count_by_genre: { [string]: number } = {}
	local song_count_by_genre: { [string]: number } = {}
	local artists_by_genre: { [string]: { [number]: boolean } } = {}
	local items: { ItemInfo } = {}
	local raw_releases = helpers.call_module_get(releases.list, scope)

	local release_ids: { number } = {}
	for _, release: Release in raw_releases do
		if release.db_id then
			table.insert(release_ids, release.db_id)
		end
	end

	local all_genres = if #release_ids > 0 then genres.get_for_releases_many(release_ids) else {}
	local all_tracks = if #release_ids > 0 then tracks.list_many(release_ids) else {}
	local all_artists = if #release_ids > 0 then artists_module.list_many(release_ids) else {}

	for _, release: Release in raw_releases do
		if not release.db_id then
			continue
		end
		local release_genres = all_genres[release.db_id]
		local release_tracks: { Track } = all_tracks[release.db_id] or {}
		local release_runtime_ticks = helpers.sum_runtime_ticks(release_tracks) or 0
		local release_artists = all_artists[release.db_id]
		if release_genres then
			for _, genre in release_genres do
				local genre_name: string = genre.name
				local normalized = helpers.lower_or_empty(genre_name)
				if normalized ~= "" then
					if runtime_ticks_by_genre[normalized] == nil then
						runtime_ticks_by_genre[normalized] = 0
						release_count_by_genre[normalized] = 0
						song_count_by_genre[normalized] = 0
						artists_by_genre[normalized] = {}
					end
					runtime_ticks_by_genre[normalized] += release_runtime_ticks
					release_count_by_genre[normalized] += 1
					song_count_by_genre[normalized] += #release_tracks

					if release_artists then
						for _, artist: Artist in release_artists do
							if artist.db_id and artist.db_id > 0 then
								artists_by_genre[normalized][artist.db_id] = true
							end
						end
					end

					if not unique[normalized] then
						unique[normalized] = { name = genre_name, id = genre.id, db_id = genre.db_id }
					end
				end
			end
		end
	end

	local genre_db_ids: { number } = {}
	for _, genre_entry in unique do
		if genre_entry.db_id and genre_entry.db_id > 0 then
			table.insert(genre_db_ids, genre_entry.db_id)
		end
	end
	local releases_by_genre_id = if #genre_db_ids > 0 then genres.get_releases_many(genre_db_ids) else {}

	for normalized_name: string, genre_entry in unique do
		local runtime_ticks: number? = runtime_ticks_by_genre[normalized_name]
		if runtime_ticks == 0 then
			runtime_ticks = nil
		end
		local artist_set = artists_by_genre[normalized_name]
		local artist_count = 0
		if artist_set then
			for _ in artist_set do
				artist_count += 1
			end
		end
		local counts: GenreCounts = {
			release_count = release_count_by_genre[normalized_name] or 0,
			song_count = song_count_by_genre[normalized_name] or 0,
			artist_count = artist_count,
		}

		local image_tag: string? = nil
		if genre_entry.db_id then
			local genre_release_ids = releases_by_genre_id[genre_entry.db_id]
			if genre_release_ids and #genre_release_ids > 0 then
				table.sort(genre_release_ids)
				local picked = helpers.deterministic_pick(genre_release_ids, genre_entry.id, 4)
				image_tag = helpers.deterministic_image_tag(picked, genre_entry.id)
			end
		end

		local genre_item = helpers.to_genre_item(
			genre_entry.name,
			genre_entry.id,
			runtime_ticks,
			counts,
			helpers.resolve_library_hex_id_from_parent(options.parent_id),
			image_tag
		)
		if genre_item then
			table.insert(items, genre_item)
		end
	end

	items = module.apply_search_filter(items, options.search_term)
	items = helpers.apply_ids_filter(items, options.ids)
	items = helpers.apply_exclude_ids_filter(items, options.exclude_item_ids)
	module.apply_sort(items, options)
	local paged_items, total_count, start_index = module.paginate_items(items, options.start_index, options.limit)
	return paged_items, total_count, start_index
end

-- Resolve a composed cover image for a genre by its db_id.
-- Used by the image endpoint to serve genre covers on demand.
function module.resolve_genre_cover(genre_db_id: number): base.ResolvedCoverInfo?
	local ok, raw_release_ids = pcall(genres.get_releases, genre_db_id)
	if not ok or not raw_release_ids then
		return nil
	end
	local release_ids = raw_release_ids :: { number }
	if #release_ids == 0 then
		return nil
	end

	local genre_nanoid = ids_module.get_id(genre_db_id)
	if not genre_nanoid then
		return nil
	end

	return helpers.compose_cover_from_ids(release_ids, genre_nanoid)
end

-- Resolve a composed cover image for a playlist by its db_id.
-- Used by the image endpoint to serve playlist covers on demand.
function module.resolve_playlist_cover(playlist_db_id: number): base.ResolvedCoverInfo?
	local playlist_info = module.find_playlist_by_id(playlist_db_id)
	if not playlist_info then
		return nil
	end

	return helpers.resolve_playlist_cover(playlist_info)
end

local ANCESTORS_MAX_DEPTH = 8
local NAME_LOOKUP_PAGE_SIZE = 200
-- Caps DB walk per name-miss; path is attacker-controlled so unbounded pagination amplifies.
local NAME_LOOKUP_MAX_PAGES = 5

function helpers.find_named_item(items: { ItemInfo }, lowered_name: string): ItemInfo?
	for _, item in items do
		local name = item.Name
		if type(name) == "string" and string.lower(name) == lowered_name then
			return item
		end
	end
	return nil
end

function helpers.paginate_name_lookup(
	name: string,
	user_id: number,
	list_fn: (ItemsQueryOptions) -> ({ ItemInfo }, number, number)
): ItemInfo?
	local lowered_name = string.lower(name)
	local options = module.with_user_query_options(module.coerce_items_query(nil), user_id)
	options.search_term = name
	options.limit = NAME_LOOKUP_PAGE_SIZE
	options.start_index = 0

	for _ = 1, NAME_LOOKUP_MAX_PAGES do
		local items, total_count = list_fn(options)
		local found = helpers.find_named_item(items, lowered_name)
		if found then
			return found
		end
		options.start_index += #items
		if #items == 0 or options.start_index >= total_count then
			return nil
		end
	end
	return nil
end

function module.find_artist_by_name(name: string, user_id: number): ItemInfo?
	return helpers.paginate_name_lookup(name, user_id, module.list_artist_items)
end

-- Single page since list_genre_items is exhaustive; pagination would be wasted.
function module.find_genre_by_name(name: string, user_id: number): ItemInfo?
	local options = module.with_user_query_options(module.coerce_items_query(nil), user_id)
	options.search_term = name
	options.limit = NAME_LOOKUP_PAGE_SIZE * NAME_LOOKUP_MAX_PAGES
	options.start_index = 0
	local items = module.list_genre_items(options)
	return helpers.find_named_item(items, string.lower(name))
end

-- Visited-set prevents cycle duplicates; boolean return distinguishes "missing" from "no ancestors".
function module.ancestors_chain(item_id: number, user_id: number): ({ ItemInfo }, boolean)
	local chain: { ItemInfo } = {}
	if item_id <= 0 then
		return chain, false
	end

	local current = module.find_item_by_id(item_id, user_id)
	if not current then
		return chain, false
	end

	local visited: { [number]: boolean } = { [item_id] = true }
	local depth = 0
	while current and depth < ANCESTORS_MAX_DEPTH do
		local parent_raw = current.ParentId
		if type(parent_raw) ~= "string" then
			break
		end
		local parent_db_id = helpers.decode_jellyfin_item_id(parent_raw)
		if not parent_db_id or visited[parent_db_id] then
			break
		end
		visited[parent_db_id] = true
		local parent = module.find_item_by_id(parent_db_id, user_id)
		if not parent then
			break
		end
		table.insert(chain, parent)
		current = parent
		depth += 1
	end
	return chain, true
end

return module
