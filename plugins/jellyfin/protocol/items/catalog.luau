--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists_module = require("@lyra/artists")
local entities = require("@lyra/entities")
local favorites = require("@lyra/favorites")
local genres = require("@lyra/genres")
local listens = require("@lyra/listens")
local lyrics = require("@lyra/lyrics")
local playback_sources = require("@lyra/playback_sources")
local playlists = require("@lyra/playlists")
local releases = require("@lyra/releases")
local tags_module = require("@lyra/tags")
local tracks = require("@lyra/tracks")

local serde = require("@harmony/serde")

local constants = require("../../constants")
local libraries = require("@lyra/libraries")
local playlist_access = require("../playlist_access")

local FAVORITE_ENTITY_KINDS = { "track", "release", "artist", "playlist" }

local artwork = require("./images")
local enrichment = require("./enrichment")
local item_ids = require("./identifiers")
local item_types = require("./types")
local lookups = require("./lookups")
local media = require("./media")
local options_module = require("./options")
local provider_ids = require("./provider_ids")
local requests = require("./requests")
local results = require("./results")
local serialization = require("./serialization")
local user_data = require("./user_data")

-- Internal helpers table to reduce top-level local count.
local helpers = {}

function helpers.is_library_root(hex_id: string): boolean
	local nanoid = item_ids.hex_to_nanoid(hex_id)
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
type TrackProjectionInfo = entities.TrackProjectionInfo
type CreditedArtistProjectionInfo = entities.CreditedArtistProjectionInfo
type CreditType = entities.CreditType
type ArtistCreditSource = entities.ArtistCreditSource

local CreditType = entities.CreditType
local ArtistCreditSource = entities.ArtistCreditSource
type ArtistRelationInfo = artists_module.ArtistRelationInfo
type Track = tracks.Track
type Release = releases.Release
-- Releases list through catalog queries, which carry their counts.
type ReleaseSummary = releases.ReleaseSummary
type Artist = artists_module.Artist
type Playlist = playlists.Playlist
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type TagInfo = tags_module.TagInfo
type TagInfoMap = { [string]: { TagInfo } }
type UserData = user_data.UserData
type ReleaseRecord = {
	id: index<Release, "id">,
	release_title: index<Release, "release_title">,
	sort_title: index<Release, "sort_title">,
	release_date: index<Release, "release_date">,
	created_at_ms: index<Release, "created_at_ms">,
	file_created_at_ms: index<Release, "file_created_at_ms">,
}
type TrackRecord = {
	id: index<Track, "id">,
	track_title: index<Track, "track_title">,
	sort_title: index<Track, "sort_title">,
	year: index<Track, "year">,
	disc: index<Track, "disc">,
	track: index<Track, "track">,
	duration_ms: index<Track, "duration_ms">,
	track_gain_db: index<Track, "track_gain_db">,
	album_gain_db: index<Track, "album_gain_db">,
	created_at_ms: index<Track, "created_at_ms">,
	file_created_at_ms: index<Track, "file_created_at_ms">,
}
type ReleaseLike = Release | ReleaseRecord
-- Artists list through catalog queries, which carry their counts.
type ArtistLike = artists_module.ArtistSummary
type TrackLike = Track | TrackRecord
type ItemRaw = ReleaseLike | ArtistLike | TrackLike | Playlist
type ItemKind = "release" | "artist" | "track" | "playlist"
type LibraryInfo = {
	id: string,
	name: string,
	path: string?,
	language: string?,
	country: string?,
}
type FavoriteLookup = options_module.FavoriteLookup
type ItemsQueryOptions = options_module.ItemsQueryOptions
type CoverInfo = {
	path: string,
	mime_type: string,
	hash: string,
	blurhash: string?,
	owner_id: string?,
}
type ArtistRelationMap = { [string]: { Artist }? }
type ArtistRelationInfoMap = { [string]: { ArtistRelationInfo }? }
type ReleaseRelationMap = { [string]: { Release }? }
type TrackRelationMap = { [string]: { Track }? }
type CreditRelationMap = { [string]: { CreditedArtistProjectionInfo }? }
type PlaybackSourceMap = { [string]: PlaybackSourceInfo }
type LyricsLookup = { [string]: boolean }
type PlayCountLookup = { [string]: number }
type LastPlayedLookup = { [string]: number }
type ItemContext = {
	user_id: string?,
	favorite_lookup: FavoriteLookup,
	play_count_lookup: PlayCountLookup,
	last_played_lookup: LastPlayedLookup,
	include_user_data: boolean,
	include_images: boolean,
	include_people: boolean,
	include_overview: boolean,
	include_parent_id: boolean,
	include_provider_ids: boolean,
	include_external_urls: boolean,
	related_artists_by_owner_id: ArtistRelationMap,
	artist_relations_by_artist_id: ArtistRelationInfoMap,
	credited_artists_by_owner_id: CreditRelationMap,
	release_tracks_by_release_id: TrackRelationMap,
	track_releases_by_track_id: ReleaseRelationMap,
	playback_source_by_track_id: PlaybackSourceMap,
	lyrics_by_track_id: LyricsLookup,
	track_release_lookup_complete: boolean?,
	has_source_prefetch: boolean?,
	has_lyrics_prefetch: boolean?,
	has_track_artists_prefetch: boolean?,
	has_track_releases_prefetch: boolean?,
	has_release_artists_prefetch: boolean?,
	has_release_tracks_prefetch: boolean?,
	has_artist_relations_prefetch: boolean?,
	genre_infos_by_release_id: { [string]: { genres.GenreInfo } }?,
	tag_infos_by_item_id: TagInfoMap?,
	cover_info_by_release_id: { [string]: artwork.ResolvedCoverInfo }?,
	cover_prefetch_release_ids: { [string]: boolean }?,
	cover_info_by_artist_id: { [string]: artwork.ResolvedCoverInfo }?,
	cover_prefetch_artist_ids: { [string]: boolean }?,
	library_hex_id: string?,
	library_hex_id_by_entity_id: { [string]: string }?,
	cover_info_by_playlist_id: { [string]: artwork.ResolvedCoverInfo }?,
	item_provider_ids: provider_ids.ItemProviderIds?,
	runtime_ticks_by_playlist_id: { [string]: number }?,
}
type RealizationSeedInfo = {
	track_ids: { string },
	release_ids: { string },
	artist_ids: { string },
	playlist_ids: { string },
	track_seen: { [string]: boolean },
	release_seen: { [string]: boolean },
	artist_seen: { [string]: boolean },
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

local module = {}

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
	return results.build_query_result(module.items_to_json(items), total_count, start_index)
end

-- Writes the query result `build_item_query_result` builds, for the items `each_batch` hands over a
-- batch at a time before returning the total and the start, so the result never exists whole.
function module.write_item_query_result(
	writer: api.ApiStreamWriter,
	each_batch: (emit: ({ ItemInfo }) -> ()) -> (number, number)
)
	local encoder = serde.json.encoder()
	encoder:begin_object()
	encoder:begin_array("Items")
	local total_count, start_index = each_batch(function(items)
		encoder:items(module.items_to_json(items))
		items = nil :: any
		writer:write(encoder:flush())
	end)
	encoder:end_array()
	-- In key order, as the whole result encodes.
	encoder:field("StartIndex", start_index)
	encoder:field("TotalRecordCount", total_count)
	encoder:end_object()
	writer:write(encoder:finish())
end

function helpers.resolve_cover_info(release_id: string?, context: ItemContext?): artwork.ResolvedCoverInfo?
	if release_id == nil then
		return nil
	end
	if context then
		if context.cover_info_by_release_id then
			local cached = context.cover_info_by_release_id[release_id]
			if cached then
				return cached
			end
		end
		-- If this release was part of the prefetch set, it has no cover.
		if context.cover_prefetch_release_ids and context.cover_prefetch_release_ids[release_id] then
			return nil
		end
	end
	return artwork.get_cover_info(release_id)
end

function helpers.resolve_artist_cover_info(artist_id: string?, context: ItemContext?): artwork.ResolvedCoverInfo?
	if artist_id == nil then
		return nil
	end
	if context then
		if context.cover_info_by_artist_id then
			local cached = context.cover_info_by_artist_id[artist_id]
			if cached then
				return cached
			end
		end
		if context.cover_prefetch_artist_ids and context.cover_prefetch_artist_ids[artist_id] then
			return nil
		end
	end
	return artwork.get_cover_info(artist_id)
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
	item_id: string?,
	item_hex_id: string?,
	user_id: string?,
	key: string?,
	include_play_count: boolean,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): UserData?
	if not helpers.should_include_user_data(context) then
		return nil
	end

	return user_data.get_user_data({
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

function helpers.lower_or_empty(value: JsonValue): string
	if value == nil then
		return ""
	end

	return string.lower(tostring(value))
end

function helpers.resolve_release_genres(release_id: string?): { genres.GenreInfo }?
	if release_id == nil then
		return nil
	end
	local infos = genres.by_release({ release_id })[release_id]
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
		local name = requests.trim(info.tag)
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
		local hex_id = item_ids.nanoid_to_hex(g.id)
		if hex_id then
			table.insert(items, { Name = g.name, Id = hex_id })
		end
	end
	return items
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
	local artist = raw :: ArtistLike?
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

function helpers.playlist_info_from_raw(raw: ItemRaw?): Playlist?
	local playlist = raw :: Playlist?
	if playlist ~= nil and type(playlist.name) == "string" then
		return playlist
	end

	return nil
end

function helpers.unix_ms_to_iso8601(unix_ms: number | string?): string?
	local ms = tonumber(unix_ms)
	if not ms then
		return nil
	end
	local s = math.floor(ms / 1000)
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

function helpers.resolve_entity_library_hex_id(entity_id: string?, context: ItemContext?): string?
	local _, hex = helpers.resolve_entity_library_attribution(entity_id, context)
	return hex
end

-- (in_library, library_hex_id). Truth is independent of encode success so callers gating
-- IsFolder on it survive a transient encode failure; encode failure on a scoped entity warns.
function helpers.resolve_entity_library_attribution(entity_id: string?, context: ItemContext?): (boolean, string?)
	if context and context.library_hex_id then
		return true, context.library_hex_id
	end
	if entity_id and context and context.library_hex_id_by_entity_id then
		local cached = context.library_hex_id_by_entity_id[entity_id]
		if cached then
			return true, cached
		end
	end
	if not entity_id then
		return false, nil
	end
	local ok, by_entity = pcall(libraries.by_entity, { entity_id })
	if not ok then
		warn(`libraries.by_entity failed for entity {entity_id}: {by_entity}`)
		return false, nil
	end
	local libs = (by_entity :: { [string]: { libraries.Library } })[entity_id]
	if not libs then
		return false, nil
	end
	local first = libs[1]
	local hex = item_ids.nanoid_to_hex(first.id)
	if hex == nil then
		warn(`nanoid_to_hex failed for library id of entity {entity_id}; ParentId will be nil`)
		return true, nil
	end
	return true, hex
end

function helpers.lookup_by_id<T>(lookup: { [string]: T }?, id: string?): T?
	if type(lookup) ~= "table" or id == nil then
		return nil
	end
	return lookup[id]
end

function helpers.append_unique_id(id: string?, output: { string }, seen: { [string]: boolean })
	if id ~= nil and not seen[id] then
		seen[id] = true
		table.insert(output, id)
	end
end

function helpers.normalize_tag_info_map(target_ids: { string }, raw_tag_map: TagInfoMap?): TagInfoMap
	local mapped: TagInfoMap = {}
	for _, target_id: string in target_ids do
		mapped[target_id] = {}
	end
	if type(raw_tag_map) ~= "table" then
		return mapped
	end

	for target_id, infos in raw_tag_map do
		if type(infos) == "table" then
			mapped[target_id] = infos
		end
	end
	return mapped
end

function helpers.fetch_tag_info_map(user_id: string?, target_ids: { string }): TagInfoMap
	if #target_ids == 0 then
		return {}
	end
	if user_id == nil then
		return helpers.normalize_tag_info_map(target_ids, nil)
	end

	local ok, raw_tag_map = pcall(tags_module.get_for_targets_many, target_ids)
	if not ok or type(raw_tag_map) ~= "table" then
		return helpers.normalize_tag_info_map(target_ids, nil)
	end

	return helpers.normalize_tag_info_map(target_ids, raw_tag_map :: TagInfoMap)
end

function helpers.fetch_tag_infos(user_id: string?, target_id: string?): { TagInfo }?
	if user_id == nil or target_id == nil then
		return nil
	end

	local ok, raw_tags = pcall(tags_module.get_for_target, target_id)
	if not ok or type(raw_tags) ~= "table" then
		return nil
	end

	return raw_tags :: { TagInfo }
end

function helpers.resolve_tag_infos(target_id: string?, context: ItemContext?): { TagInfo }?
	if target_id == nil then
		return nil
	end

	if context and context.tag_infos_by_item_id then
		local cached = context.tag_infos_by_item_id[target_id]
		if type(cached) == "table" then
			return cached
		end
	end

	return helpers.fetch_tag_infos(if context then context.user_id else nil, target_id)
end

function helpers.query_many_projections(
	ids: { string },
	include_values: { string }?
): { [string]: EntityProjectionInfo }
	if #ids == 0 then
		return {}
	end
	if type(entities.query_many) ~= "function" then
		return {}
	end

	local request: EntityQueryManyRequest = { ids = ids }
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

function helpers.query_credits(entity_id: string, entity_type: "release" | "track"): { CreditedArtistProjectionInfo }?
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

function helpers.query_artist_relations(artist_id: string): { ArtistRelationInfo }?
	local ok, relation_map = pcall(artists_module.relations, { artist_id })
	if not ok or type(relation_map) ~= "table" then
		return nil
	end

	local relations = relation_map[artist_id]
	if type(relations) ~= "table" then
		return nil
	end

	return relations :: { ArtistRelationInfo }
end

function helpers.merge_relation_map<T>(destination: { [string]: { T }? }, relation_map: { [string]: { T } })
	for key, value in relation_map do
		destination[key] = value
	end
end

function helpers.ensure_relation_entries<T>(relation_map: { [string]: { T }? }, ids: { string })
	for _, id: string in ids do
		if type(relation_map[id]) ~= "table" then
			relation_map[id] = {}
		end
	end
end

function helpers.relation_map_has_entries<T>(relation_map: { [string]: { T } }): boolean
	for _, value in relation_map do
		if type(value) == "table" and #value > 0 then
			return true
		end
	end

	return false
end

function helpers.album_gain_db(track_items: { Track }): number?
	for _, track: Track in track_items do
		if track.album_gain_db ~= nil then
			return track.album_gain_db
		end
	end
	return nil
end

function helpers.build_media_info(
	track: TrackLike?,
	source: PlaybackSourceInfo?,
	item_id: string?,
	fallback_name: string?,
	has_lyrics: boolean?
): ({ MediaSourceInfo }, { MediaStreamInfo }, string?)
	local entry = if source then source.entry else nil
	local media_item_id = item_ids.encode_jellyfin_item_id(item_id)
	local container = media.jellyfin_container(source)
	local codec = media.jellyfin_codec(source)
	local trimmed = media.is_trimmed(source)
	local size = if entry and entry.size and not trimmed then tonumber(entry.size) else nil
	local bitrate = if source then source.bitrate_bps else nil
	local direct_playable = source ~= nil and media_item_id ~= nil and not trimmed

	local media_stream: MediaStreamInfo = {
		Codec = codec,
		Type = "Audio",
		Index = 0,
		Channels = if source then source.channel_count else nil,
		SampleRate = if source then source.sample_rate_hz else nil,
		BitDepth = if source then source.bit_depth else nil,
		IsDefault = true,
		IsForced = false,
		IsHearingImpaired = false,
		IsOriginal = false,
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
	if has_lyrics then
		local lyric_stream: MediaStreamInfo = {
			Type = "Lyric",
			Index = 1,
			IsDefault = false,
			IsForced = false,
			IsHearingImpaired = false,
			IsOriginal = false,
			IsInterlaced = false,
			IsExternal = false,
			IsTextSubtitleStream = false,
			SupportsExternalStream = false,
		}
		table.insert(media_streams, lyric_stream)
		table.insert(media_stream_values, lyric_stream)
	end

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
		RunTimeTicks = if track then enrichment.ms_to_ticks(track.duration_ms) else nil,
		ReadAtNativeFramerate = false,
		IgnoreDts = false,
		IgnoreIndex = false,
		GenPtsInput = false,
		SupportsTranscoding = true,
		SupportsDirectStream = direct_playable,
		SupportsDirectPlay = direct_playable,
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

	return { media_source }, media_streams, container
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
		local artist_name = requests.trim(artist.artist_name)
		local artist_id = item_ids.encode_jellyfin_item_id(artist.id)
		if artist_name == nil or artist_id == nil then
			continue
		end

		local credit = credited_artist.credit
		local person_type = helpers.person_type_for_credit(credit.type, credited_artist.source)
		local role = requests.trim(credit.detail)
		local key = `{artist_id}\t{person_type}\t{role or ""}`
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

function helpers.people_from_artist_relations(relations: { ArtistRelationInfo }?): { PersonItem }
	local people: { PersonItem } = {}
	local seen: { [string]: boolean } = {}
	if relations == nil then
		return people
	end

	for _, relation: ArtistRelationInfo in relations do
		if helpers.lower_or_empty(relation.direction) ~= "incoming" then
			continue
		end
		if relation.relation_type ~= "voice_actor" then
			continue
		end

		local artist = relation.artist
		if artist.artist_type ~= "person" then
			continue
		end

		local artist_name = requests.trim(artist.artist_name)
		local artist_id = item_ids.encode_jellyfin_item_id(artist.id)
		if artist_name == nil or artist_id == nil or seen[artist_id] then
			continue
		end
		seen[artist_id] = true

		local person: PersonItem = {
			Name = artist_name,
			Id = artist_id,
			Type = "Actor",
		}
		table.insert(people, person)
	end

	return people
end

function helpers.resolve_people_for_item(
	entity_id: string?,
	entity_type: "release" | "track",
	context: ItemContext?
): { PersonItem }?
	if context == nil or context.include_people ~= true then
		return nil
	end
	if entity_id == nil then
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

function helpers.resolve_people_for_artist(artist_id: string?, context: ItemContext?): { PersonItem }?
	if context == nil or context.include_people ~= true then
		return nil
	end
	if artist_id == nil then
		return {}
	end

	local relations = helpers.lookup_by_id(context.artist_relations_by_artist_id, artist_id)
	if relations == nil then
		relations = helpers.query_artist_relations(artist_id)
		if relations ~= nil then
			context.artist_relations_by_artist_id[artist_id] = relations
		end
	end

	return helpers.people_from_artist_relations(relations)
end

function helpers.collect_artist_projection(
	entity_id: string?,
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
		local artist_name = requests.trim(artist.artist_name)
		local artist_id = item_ids.encode_jellyfin_item_id(artist.id)
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
	user_id: string?,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): ItemInfo?
	if not artist then
		return nil
	end

	local sort_name = requests.trim(artist.sort_name) or requests.trim(artist.artist_name) or "Unknown Artist"
	local artist_item_id = item_ids.encode_jellyfin_item_id(artist.id)
	local include_images = helpers.should_include_images(context)
	local cover_info = if include_images then helpers.resolve_artist_cover_info(artist.id, context) else nil
	-- Mirrors upstream `MusicArtist.IsFolder => !IsAccessedByName`.
	local in_library, parent_id = helpers.resolve_entity_library_attribution(artist.id, context)
	local item: ItemInfo = {
		Id = artist_item_id,
		Name = requests.trim(artist.artist_name) or "Unknown Artist",
		Type = "MusicArtist",
		IsFolder = in_library,
		ParentId = if context == nil or context.include_parent_id == true then parent_id else nil,
		SortName = sort_name,
		RunTimeTicks = enrichment.ms_to_ticks(artist.total_duration),
		DateCreated = helpers.unix_ms_to_iso8601(artist.created_at_ms),
		AlbumCount = artist.release_count,
		SongCount = artist.track_count,
		ImageTags = if cover_info then { Primary = cover_info.hash } else nil,
		ImageBlurHashes = helpers.default_image_blur_hashes(cover_info),
		UserData = helpers.build_item_user_data(
			artist.id,
			artist_item_id,
			user_id,
			`Artist-{requests.trim(artist.artist_name) or ""}`,
			false,
			favorite_lookup,
			context
		),
		People = helpers.resolve_people_for_artist(artist.id, context),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		Tags = helpers.tag_names(helpers.resolve_tag_infos(artist.id, context)),
		Overview = if context == nil or context.include_overview == true
			then requests.trim(artist.description)
			else nil,
	}
	return item
end

function helpers.to_person_item(
	artist: ArtistLike?,
	user_id: string?,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): ItemInfo?
	local item = helpers.to_artist_item(artist, user_id, favorite_lookup, context)
	if item == nil then
		return nil
	end

	item.Type = "Person"
	-- Person extends BaseItem (IsFolder=false), not Folder. Override the artist default.
	item.IsFolder = false
	return item
end

export type GenreCounts = {
	release_count: number,
	song_count: number,
}

function helpers.to_genre_item(
	genre_name: string?,
	genre_id: string?,
	counts: GenreCounts?,
	library_hex_id: string?,
	cover_info: artwork.ResolvedCoverInfo?
): ItemInfo?
	local safe_name = requests.trim(genre_name)
	if not safe_name or not genre_id then
		return nil
	end

	local hex_id = item_ids.nanoid_to_hex(genre_id)
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
		AlbumCount = if counts then counts.release_count else nil,
		SongCount = if counts then counts.song_count else nil,
		ChildCount = if counts then counts.release_count + counts.song_count else nil,
		UserData = user_data.get_user_data({ key = `MusicGenre-{safe_name}`, item_hex_id = hex_id }),
		ImageTags = if cover_info then { Primary = cover_info.hash } else nil,
		ImageBlurHashes = helpers.default_image_blur_hashes(cover_info),
		LocationType = "FileSystem",
		MediaType = "Unknown",
	}
	return item
end

-- Upstream counts a library's top-level entries only when asked; a Lyra library's nearest
-- equivalent is its albums.
function helpers.to_library_view_item(lib: LibraryInfo?, include_child_count: boolean): ItemInfo?
	if not lib then
		return nil
	end
	local hex_id = item_ids.nanoid_to_hex(lib.id)
	if not hex_id then
		return nil
	end

	return {
		Id = hex_id,
		Name = lib.name,
		SortName = lib.name,
		Type = "CollectionFolder",
		CollectionType = "music",
		IsFolder = true,
		UserData = user_data.get_user_data({ key = hex_id, item_hex_id = constants.ROOT_ITEM_ID }),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		DisplayPreferencesId = hex_id,
		Path = lib.path,
		Overview = "",
		BackdropImageTags = {},
		ParentId = constants.ROOT_ITEM_ID,
		Genres = {},
		GenreItems = {},
		Tags = {},
		Taglines = {},
		LockedFields = {},
		Studios = {},
		ExternalUrls = {},
		RemoteTrailers = {},
		People = {},
		CanDelete = false,
		CanDownload = false,
		EnableMediaSourceDisplay = true,
		LocalTrailerCount = 0,
		LockData = false,
		PlayAccess = "Full",
		SpecialFeatureCount = 0,
		ChildCount = if include_child_count then releases.query({ library_id = lib.id, limit = 0 }).total else nil,
	} :: ItemInfo
end

function module.to_music_view_items(include_child_count: boolean): { ItemInfo }
	local ok, libs = pcall(libraries.list)
	if not ok or type(libs) ~= "table" then
		return {}
	end
	local items: { ItemInfo } = {}
	for _, lib in libs :: { LibraryInfo } do
		local item = helpers.to_library_view_item(lib, include_child_count)
		if item then
			table.insert(items, item)
		end
	end
	return items
end

-- Where upstream keeps the playlists it creates, `<DataPath>/playlists`. Lyra's playlists aren't
-- files, but clients tell a server's own playlists apart by this path, as Jellify does.
local PLAYLISTS_PATH = "/config/data/playlists"

-- A playlist's folder as upstream names it: the name with the characters its file system rejects
-- replaced by spaces.
function helpers.playlist_path(name: string): string
	return `{PLAYLISTS_PATH}/{(string.gsub(name, '[%z\1-\31"<>|:%*%?\\/]', " "))}`
end

-- Upstream's playlists view, the folder its playlists live in.
function module.playlists_folder_item(child_count: number?): ItemInfo
	local id = constants.PLAYLISTS_FOLDER_ID
	return {
		Id = id,
		Name = "Playlists",
		SortName = "playlists",
		Type = "ManualPlaylistsFolder",
		CollectionType = "playlists",
		Path = PLAYLISTS_PATH,
		IsFolder = true,
		UserData = user_data.get_user_data({ key = id, item_hex_id = id }),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		DisplayPreferencesId = id,
		ParentId = constants.ROOT_ITEM_ID,
		ChildCount = child_count,
	} :: ItemInfo
end

function helpers.to_playlist_item(
	playlist: Playlist?,
	user_id: string?,
	child_count: number?,
	context: ItemContext?
): ItemInfo?
	if not playlist then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local playlist_name = requests.trim(playlist.name) or "Untitled Playlist"
	local playlist_item_id = item_ids.encode_jellyfin_item_id(playlist.id)

	local cover_info: artwork.ResolvedCoverInfo? = nil
	if include_images then
		cover_info = if context and context.cover_info_by_playlist_id
			then context.cover_info_by_playlist_id[playlist.id]
			else artwork.get_artwork_info(playlist.id)
	end

	local item: ItemInfo = {
		Id = playlist_item_id,
		Name = playlist_name,
		Type = "Playlist",
		SortName = playlist_name,
		Path = helpers.playlist_path(playlist_name),
		ChildCount = child_count,
		RunTimeTicks = helpers.lookup_by_id(if context then context.runtime_ticks_by_playlist_id else nil, playlist.id)
			or enrichment.playlist_runtime_ticks({ playlist.id })[playlist.id],
		DateCreated = helpers.unix_ms_to_iso8601(playlist.created_at_ms),
		ImageTags = if cover_info then { Primary = cover_info.hash } else nil,
		ImageBlurHashes = helpers.default_image_blur_hashes(cover_info),
		UserData = helpers.build_item_user_data(
			playlist.id,
			playlist_item_id,
			user_id,
			`Playlist-{playlist.name or ""}`,
			false,
			if context then context.favorite_lookup else nil,
			context
		),
		LocationType = "FileSystem",
		MediaType = "Unknown",
		Tags = helpers.tag_names(helpers.resolve_tag_infos(playlist.id, context)),
	}
	return item
end

function helpers.to_release_item(
	release: ReleaseLike?,
	user_id: string?,
	favorite_lookup: FavoriteLookup?,
	context: ItemContext?
): ItemInfo?
	if not release then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local release_artist, artist_names, artist_items = helpers.collect_artist_projection(release.id, "release", context)
	local prefetched_release_tracks =
		helpers.lookup_by_id(if context then context.release_tracks_by_release_id else nil, release.id)
	local release_tracks: { Track } = if prefetched_release_tracks ~= nil
		then prefetched_release_tracks
		else lookups.tracks_of_release(release.id)
	local runtime_ticks = enrichment.sum_runtime_ticks(release_tracks)
	-- Jellyfin exposes image tags by default, but honors EnableImages=false.
	local cover_info = if include_images then helpers.resolve_cover_info(release.id, context) else nil
	local release_item_id = item_ids.encode_jellyfin_item_id(release.id)
	local release_genres = helpers.lookup_by_id(if context then context.genre_infos_by_release_id else nil, release.id)
		or helpers.resolve_release_genres(release.id)
	local item: ItemInfo = {
		Id = release_item_id,
		Name = requests.trim(release.release_title) or "Unknown Album",
		Type = "MusicAlbum",
		IsFolder = true,
		MediaType = "Unknown",
		SortName = requests.trim(release.sort_title) or requests.trim(release.release_title) or "Unknown Album",
		ProductionYear = enrichment.release_year(release.release_date),
		PremiereDate = enrichment.release_date_to_premiere_date(release.release_date),
		DateCreated = helpers.unix_ms_to_iso8601(release.file_created_at_ms or release.created_at_ms),
		Genres = helpers.genre_names(release_genres),
		GenreItems = helpers.genre_items(release_genres),
		Tags = helpers.tag_names(helpers.resolve_tag_infos(release.id, context)),
		ParentId = if #artist_items > 0
			then artist_items[1].Id
			else helpers.resolve_entity_library_hex_id(release.id, context),
		AlbumArtist = release_artist,
		AlbumArtists = artist_items,
		Artists = artist_names,
		ArtistItems = artist_items,
		People = helpers.resolve_people_for_item(release.id, "release", context),
		ChildCount = #release_tracks,
		RunTimeTicks = runtime_ticks,
		NormalizationGain = helpers.album_gain_db(release_tracks),
		ImageTags = if cover_info then { Primary = cover_info.hash } else nil,
		AlbumPrimaryImageTag = if cover_info then cover_info.hash else nil,
		ImageBlurHashes = helpers.default_image_blur_hashes(cover_info),
		UserData = helpers.build_item_user_data(
			release.id,
			release_item_id,
			user_id,
			`{release_artist or ""}-{requests.trim(release.release_title) or ""}`,
			false,
			favorite_lookup,
			context
		),
		LocationType = "FileSystem",
	}
	return item
end

function helpers.resolve_release_for_track(
	track: TrackLike?,
	preferred_release_id: string?,
	include_images: boolean?,
	context: ItemContext?
): (Release?, CoverInfo?)
	local track_id = if track then track.id else nil
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
		raw_releases = lookups.releases_of_track(track_id)
	end
	local candidates = lookups.releases_by_added(raw_releases)
	if #candidates == 0 then
		return nil, nil
	end

	if preferred_release_id ~= nil then
		for _, release in candidates do
			if release.id == preferred_release_id then
				local preferred_cover = if include_images then helpers.resolve_cover_info(release.id, context) else nil
				return release, preferred_cover
			end
		end
	end

	local selected_release: Release? = candidates[1]
	if include_images ~= true then
		return selected_release, nil
	end

	local selected_cover_info = if selected_release
		then helpers.resolve_cover_info(selected_release.id, context)
		else nil
	for _, release: Release in candidates do
		local cover_info = helpers.resolve_cover_info(release.id, context)
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
	preferred_release_id: string?,
	user_id: string?,
	favorite_lookup: FavoriteLookup?,
	include_media_info: boolean?,
	context: ItemContext?
): ItemInfo?
	if not track then
		return nil
	end

	local include_images = helpers.should_include_images(context)
	local release, release_cover_info =
		helpers.resolve_release_for_track(track, preferred_release_id, include_images, context)
	local artist_name, artist_names, artist_items = helpers.collect_artist_projection(track.id, "track", context)
	local release_artist_name = artist_name
	local release_artist_names = artist_names
	local release_artist_items = artist_items
	if release then
		local resolved_release_artist_name, resolved_release_artist_names, resolved_release_artist_items =
			helpers.collect_artist_projection(release.id, "release", context)
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
	local has_lyrics: boolean
	if context and context.has_lyrics_prefetch then
		has_lyrics = context.lyrics_by_track_id[track.id] == true
	else
		local ok, entries = pcall(function(): { lyrics.LyricsSummary }?
			return lyrics.by_track({ track.id })[track.id]
		end)
		has_lyrics = ok and entries ~= nil and #entries > 0
	end
	local media_sources: { MediaSourceInfo }? = nil
	local media_streams: { MediaStreamInfo }? = nil
	local container = nil
	local source = nil
	if context and context.has_source_prefetch then
		source = helpers.lookup_by_id(context.playback_source_by_track_id, track.id)
	else
		source = media.resolve_playback_source_for_track(track, include_media_info == true)
	end
	if include_media_info == true then
		media_sources, media_streams, container =
			helpers.build_media_info(track, source, track.id, track.track_title, has_lyrics)
	else
		container = media.jellyfin_container(source)
	end
	local album_id = if release then item_ids.encode_jellyfin_item_id(release.id) else nil
	local track_item_id = item_ids.encode_jellyfin_item_id(track.id)
	local album_primary_image_tag = if release_cover_info then release_cover_info.hash else nil
	local release_id = if release then release.id else nil
	local release_date = if release then release.release_date else nil
	local track_genres = helpers.lookup_by_id(if context then context.genre_infos_by_release_id else nil, release_id)
		or helpers.resolve_release_genres(release_id)
	local track_sort_name = helpers.track_sort_name(track)

	local item: ItemInfo = {
		Id = track_item_id,
		Name = requests.trim(track.track_title) or "Unknown Track",
		Type = "Audio",
		IsFolder = false,
		MediaType = "Audio",
		LocationType = "FileSystem",
		Container = container,
		SortName = track_sort_name,
		RunTimeTicks = enrichment.ms_to_ticks(track.duration_ms),
		DateCreated = helpers.unix_ms_to_iso8601(track.file_created_at_ms or track.created_at_ms),
		IndexNumber = track.track,
		ParentIndexNumber = track.disc,
		ProductionYear = track.year or enrichment.release_year(release_date),
		PremiereDate = enrichment.track_premiere_date(track.year, release_date),
		Album = if release then release.release_title else nil,
		AlbumId = album_id,
		ParentId = album_id,
		AlbumArtist = release_artist_name,
		AlbumArtists = release_artist_items,
		Artists = artist_names,
		ArtistItems = artist_items,
		People = helpers.resolve_people_for_item(track.id, "track", context),
		Genres = helpers.genre_names(track_genres),
		GenreItems = helpers.genre_items(track_genres),
		Tags = helpers.tag_names(helpers.resolve_tag_infos(track.id, context)),
		AlbumPrimaryImageTag = album_primary_image_tag,
		ImageTags = helpers.default_image_tags(),
		ImageBlurHashes = helpers.default_image_blur_hashes(release_cover_info),
		HasLyrics = has_lyrics,
		NormalizationGain = track.track_gain_db,
		AlbumNormalizationGain = track.album_gain_db,
		UserData = helpers.build_item_user_data(
			track.id,
			track_item_id,
			user_id,
			`{track.disc or 0}-{track.track or 0}{requests.trim(track.track_title) or ""}`,
			true,
			favorite_lookup,
			context
		),
		MediaSources = media_sources,
		MediaStreams = media_streams,
	}
	return item
end

function helpers.track_sort_name(track: TrackLike?): string
	local title = if track then requests.trim(track.sort_title) or requests.trim(track.track_title) else nil
	local sort_name: string = if type(title) == "string" then title else "Unknown Track"
	local prefix: string = ""

	local disc = if track then tonumber(track.disc) else nil
	if disc ~= nil then
		prefix ..= string.format("%04d - ", math.floor(disc))
	end

	local track_number = if track then tonumber(track.track) else nil
	if track_number ~= nil then
		prefix ..= string.format("%04d - ", math.floor(track_number))
	end

	return prefix .. sort_name
end

function helpers.to_release_stub(release: ReleaseLike?): ItemInfo?
	if not release then
		return nil
	end
	local item: ItemInfo = {
		_type = "release",
		_raw = release,
		Id = item_ids.encode_jellyfin_item_id(release.id),
		Name = requests.trim(release.release_title) or "Unknown Album",
		SortName = requests.trim(release.sort_title) or requests.trim(release.release_title) or "Unknown Album",
		Album = requests.trim(release.release_title) or "Unknown Album",
		DateCreated = helpers.unix_ms_to_iso8601(release.file_created_at_ms or release.created_at_ms),
		PremiereDate = enrichment.release_date_to_premiere_date(release.release_date),
		ProductionYear = enrichment.release_year(release.release_date),
	}
	return item
end

function helpers.to_track_stub(track: TrackLike?): ItemInfo?
	if not track then
		return nil
	end
	local item: ItemInfo = {
		_type = "track",
		_raw = track,
		Id = item_ids.encode_jellyfin_item_id(track.id),
		Name = requests.trim(track.track_title) or "Unknown Track",
		SortName = helpers.track_sort_name(track),
		IndexNumber = track.track,
		ParentIndexNumber = track.disc,
		DateCreated = helpers.unix_ms_to_iso8601(track.file_created_at_ms or track.created_at_ms),
	}
	return item
end

function helpers.to_artist_stub(artist: ArtistLike?): ItemInfo?
	if not artist then
		return nil
	end
	local item: ItemInfo = {
		_type = "artist",
		_raw = artist,
		Id = item_ids.encode_jellyfin_item_id(artist.id),
		Name = requests.trim(artist.artist_name) or "Unknown Artist",
		SortName = requests.trim(artist.sort_name) or requests.trim(artist.artist_name) or "Unknown Artist",
		DateCreated = helpers.unix_ms_to_iso8601(artist.created_at_ms),
	}
	return item
end

function helpers.to_person_stub(artist: ArtistLike?): ItemInfo?
	local item = helpers.to_artist_stub(artist)
	if item == nil then
		return nil
	end

	item.Type = "Person"
	return item
end

function helpers.to_playlist_stub(playlist: Playlist?, child_count: number?): ItemInfo?
	if not playlist then
		return nil
	end

	local count = if child_count ~= nil then child_count else playlist_access.track_count(playlist)
	local playlist_name = requests.trim(playlist.name) or "Untitled Playlist"
	local item: ItemInfo = {
		_type = "playlist",
		_raw = playlist,
		_child_count = count,
		Id = item_ids.encode_jellyfin_item_id(playlist.id),
		Name = playlist_name,
		SortName = playlist_name,
		ChildCount = count,
		DateCreated = helpers.unix_ms_to_iso8601(playlist.created_at_ms),
	}
	return item
end

function helpers.append_release_stubs(stubs: { ItemInfo }, release_items: { ReleaseSummary }): ()
	for _, release: ReleaseSummary in release_items do
		local stub = helpers.to_release_stub(release)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_artist_stubs(stubs: { ItemInfo }, artist_items: { ArtistLike }): ()
	for _, artist: ArtistLike in artist_items do
		local stub = helpers.to_artist_stub(artist)
		if stub then
			table.insert(stubs, stub)
		end
	end
end

function helpers.append_person_stubs(stubs: { ItemInfo }, artist_items: { ArtistLike }): ()
	for _, artist: ArtistLike in artist_items do
		local stub = helpers.to_person_stub(artist)
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

function helpers.favorite_id_lookup(options: ItemsQueryOptions?): FavoriteLookup?
	if options and type(options._favorite_id_lookup) == "table" then
		return options._favorite_id_lookup
	end

	local user_id = if options then options.user_id else nil
	local lookup: FavoriteLookup = {}
	if not user_id then
		if options then
			options._favorite_id_lookup = lookup
		end
		return lookup
	end

	for _, kind in FAVORITE_ENTITY_KINDS do
		local ok, ids = pcall(favorites.list_ids, kind)
		if ok and type(ids) == "table" then
			for _, target_id in ids do
				lookup[target_id] = true
			end
		else
			-- `favorites.list_ids` errors rather than silently truncating above the server cap.
			-- Surface as a warning so power users with very large favorite sets see why the
			-- IsFavorite filter returns zero items for that kind.
			warn(`favorites.list_ids failed for user_id={user_id} kind={kind}: {tostring(ids)}`)
		end
	end

	if options then
		options._favorite_id_lookup = lookup
	end

	return lookup
end

function helpers.new_realization_context(options: ItemsQueryOptions): ItemContext
	local favorite_lookup: FavoriteLookup = {}
	if options.enable_user_data ~= false then
		favorite_lookup = helpers.favorite_id_lookup(options) or {}
	end

	return {
		user_id = options.user_id,
		favorite_lookup = favorite_lookup,
		play_count_lookup = {},
		last_played_lookup = {},
		include_user_data = options.enable_user_data ~= false,
		include_images = options.enable_images ~= false,
		include_people = options.include_people == true,
		include_overview = options.include_overview == true,
		include_parent_id = options.include_parent_id == true,
		include_provider_ids = options.include_provider_ids == true,
		include_external_urls = options.include_external_urls == true,
		related_artists_by_owner_id = {},
		artist_relations_by_artist_id = {},
		credited_artists_by_owner_id = {},
		release_tracks_by_release_id = {},
		track_releases_by_track_id = {},
		playback_source_by_track_id = {},
		lyrics_by_track_id = {},
		has_source_prefetch = false,
		has_lyrics_prefetch = false,
		tag_infos_by_item_id = options._tag_infos_by_item_id,
		library_hex_id = helpers.resolve_library_hex_id_from_parent(options.parent_id),
	}
end

function helpers.collect_realization_seed(stubs: { ItemInfo }, context: ItemContext): RealizationSeedInfo
	local seed: RealizationSeedInfo = {
		track_ids = {},
		release_ids = {},
		artist_ids = {},
		playlist_ids = {},
		track_seen = {},
		release_seen = {},
		artist_seen = {},
		has_track_stubs = false,
		has_release_stubs = false,
		has_artist_stubs = false,
	}

	for _, stub: ItemInfo in stubs do
		local raw = stub._raw
		local id = if raw then raw.id else nil
		if id ~= nil then
			context.play_count_lookup[id] = 0
		end

		if stub._type == "track" then
			seed.has_track_stubs = true
			helpers.append_unique_id(id, seed.track_ids, seed.track_seen)
		elseif stub._type == "release" then
			seed.has_release_stubs = true
			helpers.append_unique_id(id, seed.release_ids, seed.release_seen)
		elseif stub._type == "artist" then
			seed.has_artist_stubs = true
			helpers.append_unique_id(id, seed.artist_ids, seed.artist_seen)
		elseif stub._type == "playlist" and id then
			table.insert(seed.playlist_ids, id)
		end
	end

	return seed
end

function helpers.prefetch_library_hex_ids(context: ItemContext, seed: RealizationSeedInfo): ()
	if context.library_hex_id then
		return
	end

	local all_entity_ids: { string } = {}
	for _, id in seed.release_ids do
		table.insert(all_entity_ids, id)
	end
	for _, id in seed.artist_ids do
		table.insert(all_entity_ids, id)
	end
	if #all_entity_ids == 0 then
		return
	end

	local ok, lib_map = pcall(libraries.by_entity, all_entity_ids)
	if ok and lib_map then
		local hex_map: { [string]: string } = {}
		for entity_id, libs in lib_map do
			local hex = item_ids.nanoid_to_hex(libs[1].id)
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

	local track_artists_map = artists_module.by_track(seed.track_ids)
	helpers.merge_relation_map(context.related_artists_by_owner_id, track_artists_map)
	context.has_track_artists_prefetch = helpers.relation_map_has_entries(track_artists_map)
end

function helpers.prefetch_track_releases(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_track_stubs or #seed.track_ids == 0 then
		return
	end

	local track_releases_map = releases.by_track(seed.track_ids)
	helpers.merge_relation_map(context.track_releases_by_track_id, track_releases_map)
	context.has_track_releases_prefetch = helpers.relation_map_has_entries(track_releases_map)
	for _, related_releases: { Release } in track_releases_map do
		for _, release: Release in related_releases do
			helpers.append_unique_id(release.id, seed.release_ids, seed.release_seen)
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

	local prefetch_set: { [string]: boolean } = {}
	for _, release_id: string in seed.release_ids do
		prefetch_set[release_id] = true
	end
	context.cover_info_by_release_id = artwork.get_cover_infos(seed.release_ids)
	context.cover_prefetch_release_ids = prefetch_set
end

function helpers.prefetch_artist_covers(context: ItemContext, seed: RealizationSeedInfo): ()
	if context.include_images ~= true or not seed.has_artist_stubs or #seed.artist_ids == 0 then
		return
	end

	local prefetch_set: { [string]: boolean } = {}
	for _, artist_id: string in seed.artist_ids do
		prefetch_set[artist_id] = true
	end
	context.cover_info_by_artist_id = artwork.get_cover_infos(seed.artist_ids)
	context.cover_prefetch_artist_ids = prefetch_set
end

function helpers.prefetch_release_artists_and_genres(
	context: ItemContext,
	seed: RealizationSeedInfo,
	options: ItemsQueryOptions
): ()
	if (not seed.has_release_stubs and not seed.has_track_stubs) or #seed.release_ids == 0 then
		return
	end

	local release_artists_map = artists_module.by_release(seed.release_ids)
	helpers.merge_relation_map(context.related_artists_by_owner_id, release_artists_map)
	context.has_release_artists_prefetch = helpers.relation_map_has_entries(release_artists_map)

	local genre_cache: { [string]: { genres.GenreInfo } } = {}
	if type(options._genre_infos_by_release_id) == "table" then
		genre_cache = options._genre_infos_by_release_id
	else
		local all_genres = genres.by_release(seed.release_ids)
		if type(all_genres) == "table" then
			for release_id, infos in all_genres do
				if type(infos) == "table" and #infos > 0 then
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

	local ids: { string } = {}
	local seen: { [string]: boolean } = {}
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
		local credits = helpers.projection_credits(projection_lookup[id])
		if credits ~= nil then
			context.credited_artists_by_owner_id[id] = credits
		end
	end
end

function helpers.prefetch_provider_ids(context: ItemContext, seed: RealizationSeedInfo): ()
	if not context.include_provider_ids and not context.include_external_urls then
		return
	end

	local library_id = if context.library_hex_id then item_ids.decode_jellyfin_item_id(context.library_hex_id) else nil
	local index: provider_ids.ItemProviderIds? = provider_ids.fetch(
		seed,
		context.related_artists_by_owner_id,
		context.include_provider_ids,
		context.include_external_urls,
		library_id
	)
	context.item_provider_ids = index
end

function helpers.prefetch_release_tracks(context: ItemContext, seed: RealizationSeedInfo): ()
	if not seed.has_release_stubs or #seed.release_ids == 0 then
		return
	end

	local release_tracks_map = tracks.by_release(seed.release_ids)
	helpers.merge_relation_map(context.release_tracks_by_release_id, release_tracks_map)
	context.has_release_tracks_prefetch = helpers.relation_map_has_entries(release_tracks_map)
end

function helpers.prefetch_artist_people_relations(context: ItemContext, seed: RealizationSeedInfo): ()
	if context.include_people ~= true or not seed.has_artist_stubs or #seed.artist_ids == 0 then
		return
	end

	helpers.ensure_relation_entries(context.artist_relations_by_artist_id, seed.artist_ids)

	local ok, relation_map = pcall(artists_module.relations, seed.artist_ids)
	if not ok or type(relation_map) ~= "table" then
		return
	end

	helpers.merge_relation_map(
		context.artist_relations_by_artist_id,
		relation_map :: { [string]: { ArtistRelationInfo } }
	)
	context.has_artist_relations_prefetch = helpers.relation_map_has_entries(relation_map)
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
		for id, stats in listens.get(seed.track_ids) do
			context.play_count_lookup[id] = stats.count
			if stats.last_played_at_ms ~= nil then
				context.last_played_lookup[id] = stats.last_played_at_ms
			end
		end
	end

	local sources = playback_sources.get_many(seed.track_ids, options.include_media_info == true)
	if type(sources) == "table" then
		context.playback_source_by_track_id = sources
		context.has_source_prefetch = true
	end
end

function helpers.prefetch_track_lyrics(context: ItemContext, seed: RealizationSeedInfo): ()
	if #seed.track_ids == 0 then
		return
	end

	local ok, ranked = pcall(function(): { [string]: { lyrics.LyricsSummary } }
		return lyrics.by_track(seed.track_ids)
	end)
	if not ok then
		return
	end

	local lookup: LyricsLookup = {}
	for track_id, entries in ranked do
		lookup[track_id] = #entries > 0
	end
	context.lyrics_by_track_id = lookup
	context.has_lyrics_prefetch = true
end

function helpers.prefetch_playlist_covers(context: ItemContext, seed: RealizationSeedInfo): ()
	if #seed.playlist_ids == 0 or not helpers.should_include_images(context) then
		return
	end

	context.cover_info_by_playlist_id = artwork.get_artwork_infos(seed.playlist_ids)
end

function helpers.prefetch_playlist_runtimes(context: ItemContext, seed: RealizationSeedInfo): ()
	if #seed.playlist_ids == 0 then
		return
	end

	context.runtime_ticks_by_playlist_id = enrichment.playlist_runtime_ticks(seed.playlist_ids)
end

function helpers.collect_tag_target_ids(seed: RealizationSeedInfo): { string }
	local target_ids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, id: string in seed.track_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: string in seed.release_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: string in seed.artist_ids do
		helpers.append_unique_id(id, target_ids, seen)
	end
	for _, id: string in seed.playlist_ids do
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
	helpers.prefetch_artist_covers(context, seed)
	helpers.prefetch_release_artists_and_genres(context, seed, options)
	helpers.prefetch_credit_relations(context, seed)
	helpers.prefetch_provider_ids(context, seed)
	helpers.prefetch_release_tracks(context, seed)
	helpers.prefetch_artist_people_relations(context, seed)
	helpers.prefetch_track_stats_and_media(context, seed, options)
	helpers.prefetch_track_lyrics(context, seed)
	helpers.prefetch_playlist_covers(context, seed)
	helpers.prefetch_playlist_runtimes(context, seed)
	helpers.prefetch_item_tags(context, seed, options)

	return context
end

function helpers.realize_stubs(stubs: { ItemInfo }, options: ItemsQueryOptions): { ItemInfo }
	local items: { ItemInfo } = {}
	local user_id = options.user_id
	local context = helpers.build_realization_context(stubs, options)
	local favorite_lookup = context.favorite_lookup
	local parent_scope = lookups.scope(options.parent_id)
	local scoped_release_id = if parent_scope.entity_type == "Release" then parent_scope.id else nil
	for _, stub: ItemInfo in stubs do
		local item: ItemInfo? = nil
		local raw = stub._raw
		if stub._type == "release" then
			item = helpers.to_release_item(helpers.release_like_from_raw(raw), user_id, favorite_lookup, context)
		elseif stub._type == "artist" then
			if stub.Type == "Person" then
				item = helpers.to_person_item(helpers.artist_like_from_raw(raw), user_id, favorite_lookup, context)
			else
				item = helpers.to_artist_item(helpers.artist_like_from_raw(raw), user_id, favorite_lookup, context)
			end
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
			if context.item_provider_ids then
				provider_ids.apply(item, context.item_provider_ids)
			end
			table.insert(items, item)
		end
	end
	return items
end

-- Artwork routes are public, so this resolves names without a caller; it only ever leads to
-- image bytes. People are artists, so it serves both.
function module.find_artist_id_by_name(name: string): string?
	return artists_module.artwork_id_by_name(name)
end

-- Artwork routes are public, so this resolves names without a caller; it only ever leads to
-- image bytes.
function module.find_genre_id_by_name(name: string): string?
	local genre_info = genres.find_by_name(name)
	return if genre_info then genre_info.id else nil
end

module.realize = helpers.realize_stubs
module.playlist_stub = helpers.to_playlist_stub
module.genre_item = helpers.to_genre_item
module.library_hex_id_from_parent = helpers.resolve_library_hex_id_from_parent

function module.track_stubs(items: { Track }): { ItemInfo }
	local stubs: { ItemInfo } = {}
	helpers.append_track_stubs(stubs, items)
	return stubs
end

function module.release_stubs(items: { ReleaseSummary }): { ItemInfo }
	local stubs: { ItemInfo } = {}
	helpers.append_release_stubs(stubs, items)
	return stubs
end

function module.artist_stubs(items: { ArtistLike }): { ItemInfo }
	local stubs: { ItemInfo } = {}
	helpers.append_artist_stubs(stubs, items)
	return stubs
end

-- Only person artists list as people.
function module.person_stubs(items: { ArtistLike }): { ItemInfo }
	local stubs: { ItemInfo } = {}
	helpers.append_person_stubs(stubs, items)
	return stubs
end

return module
