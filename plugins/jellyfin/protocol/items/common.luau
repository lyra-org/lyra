--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local covers = require("@lyra/covers")
local favorites = require("@lyra/favorites")
local ids = require("@lyra/ids")
local listens = require("@lyra/listens")
local playback_sources = require("@lyra/playback_sources")

local constants = require("../../constants")
local encoding = require("../../encoding")

local ITEM_ID_HEX_LENGTH = constants.ITEM_ID_HEX_LENGTH
local ITEM_ID_HEX_PATTERN = constants.ITEM_ID_HEX_PATTERN
local RESOLVE_MERGE_TRACK_COUNT = constants.RESOLVE_MERGE_TRACK_COUNT

type RawCoverInfo = covers.CoverInfo
type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo
type JsonObject = { [string]: api.JsonValue }

export type ResolvedCoverInfo = {
	path: string,
	mime_type: string,
	hash: string,
	blurhash: string?,
	release_id: string?,
}

type ImageFormat = "jpg" | "png" | "webp"

export type UserDataOpts = {
	item_id: number?,
	user_id: number?,
	include_favorite: boolean?,
	include_play_count: boolean?,
	favorite_lookup: { [number]: boolean }?,
	play_count_lookup: { [number]: number }?,
	last_played_lookup: { [number]: number }?,
	key: string?,
	item_hex_id: string?,
}

export type UserData = {
	Key: string?,
	ItemId: string?,
	PlaybackPositionTicks: number,
	PlayCount: number,
	IsFavorite: boolean,
	Played: boolean,
	LastPlayedDate: string?,
}

local module = {}

local function has_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, is_favorite = pcall(favorites.has, user_id, item_id)
	return ok and is_favorite == true
end

function module.has_favorite(user_id: number?, item_id: number?): boolean
	return has_favorite(user_id, item_id)
end

function module.add_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, applied = pcall(favorites.add, user_id, item_id)
	return ok and applied == true
end

function module.remove_favorite(user_id: number?, item_id: number?): boolean
	if not user_id or user_id <= 0 or not item_id or item_id <= 0 then
		return false
	end

	local ok, applied = pcall(favorites.remove, user_id, item_id)
	return ok and applied == true
end

-- Howard Hinnant's civil_from_days algorithm
local function ms_to_iso8601(ms: number?): string?
	if not ms or ms <= 0 then
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
	local remainder = s % 86400
	local h = math.floor(remainder / 3600)
	local min = math.floor((remainder % 3600) / 60)
	local sec = remainder % 60
	return string.format("%04d-%02d-%02dT%02d:%02d:%02d.0000000Z", y, m, d, h, min, sec)
end

function module.get_user_data(opts: UserDataOpts?): UserData
	if not opts then
		return {
			PlaybackPositionTicks = 0,
			PlayCount = 0,
			IsFavorite = false,
			Played = false,
		}
	end

	local item_id = opts.item_id
	local user_id = opts.user_id

	local play_count = 0
	local should_include_play_count = opts.include_play_count ~= false
	if should_include_play_count and item_id ~= nil and item_id > 0 then
		local lookup_count: number? = nil
		if type(opts.play_count_lookup) == "table" then
			lookup_count = opts.play_count_lookup[item_id]
		end

		if lookup_count ~= nil then
			play_count = tonumber(lookup_count) or 0
		else
			local ok, count = pcall(listens.get_count, item_id, user_id, RESOLVE_MERGE_TRACK_COUNT)
			if ok then
				play_count = count
			end
		end
	end

	local is_favorite = false
	if opts.include_favorite == true then
		if type(opts.favorite_lookup) == "table" and item_id ~= nil then
			is_favorite = opts.favorite_lookup[item_id] == true
		else
			is_favorite = has_favorite(opts.user_id, item_id)
		end
	end

	local last_played_date: string? = nil
	if item_id ~= nil and item_id > 0 and type(opts.last_played_lookup) == "table" then
		last_played_date = ms_to_iso8601(opts.last_played_lookup[item_id])
	end

	return {
		Key = opts.key,
		ItemId = opts.item_hex_id,
		PlaybackPositionTicks = 0,
		PlayCount = play_count,
		IsFavorite = is_favorite,
		Played = play_count > 0,
		LastPlayedDate = last_played_date,
	}
end

function module.user_data_to_json(user_data: UserData): JsonObject
	local result: JsonObject = {
		PlaybackPositionTicks = user_data.PlaybackPositionTicks,
		PlayCount = user_data.PlayCount,
		IsFavorite = user_data.IsFavorite,
		Played = user_data.Played,
	}
	if user_data.Key ~= nil then
		result.Key = user_data.Key
	end
	if user_data.ItemId ~= nil then
		result.ItemId = user_data.ItemId
	end
	if user_data.LastPlayedDate ~= nil then
		result.LastPlayedDate = user_data.LastPlayedDate
	end
	return result
end

function module.bad_request(message: string?): api.ApiResponse
	return api.response.json(400, {
		Message = message or "Bad Request",
	})
end

function module.not_found(message: string?): api.ApiResponse
	return api.response.json(404, {
		Message = message or "Not Found",
	})
end

function module.forbidden(message: string?): api.ApiResponse
	return api.response.json(403, {
		Message = message or "Forbidden",
	})
end

function module.trim(value: string?): string?
	if type(value) ~= "string" then
		return nil
	end

	local trimmed = string.gsub(value, "^%s*(.-)%s*$", "%1")
	if trimmed == "" then
		return nil
	end

	return trimmed
end

function module.unquote(value: string?): string?
	local text = module.trim(value)
	if not text then
		return nil
	end

	if string.sub(text, 1, 1) == '"' and string.sub(text, -1) == '"' and #text >= 2 then
		return string.sub(text, 2, -2)
	end

	return text
end

function module.split_csv_preserving_quotes(raw: string): { string }
	local parts: { string } = {}
	local current_parts: { string } = {}
	local in_quotes = false

	for index = 1, #raw do
		local char = string.sub(raw, index, index)
		if char == '"' then
			in_quotes = not in_quotes
			table.insert(current_parts, char)
		elseif char == "," and not in_quotes then
			table.insert(parts, table.concat(current_parts))
			current_parts = {}
		else
			table.insert(current_parts, char)
		end
	end

	local final = table.concat(current_parts)
	if final ~= "" then
		table.insert(parts, final)
	end

	return parts
end

local function split_csv(raw: string?): { string }
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

local function lower_or_empty(value: string | number | boolean?): string
	if value == nil then
		return ""
	end

	return string.lower(tostring(value))
end

local function parse_host_without_port(raw_host: string?): string?
	local host = module.trim(raw_host)
	if not host then
		return nil
	end

	if string.sub(host, 1, 1) == "[" then
		local close_bracket = string.find(host, "]", 1, true)
		if close_bracket then
			local ipv6 = string.sub(host, 2, close_bracket - 1)
			return lower_or_empty(ipv6)
		end
	end

	local ipv4_or_name = string.match(host, "^[^:]+")
	return lower_or_empty(ipv4_or_name or host)
end

local function is_private_ipv4(host: string): boolean
	local a, b = string.match(host, "^(%d+)%.(%d+)%.%d+%.%d+$")
	if not a or not b then
		return false
	end

	local first = tonumber(a)
	local second = tonumber(b)
	if not first or not second then
		return false
	end

	if first == 10 or first == 127 then
		return true
	end
	if first == 192 and second == 168 then
		return true
	end
	if first == 172 and second >= 16 and second <= 31 then
		return true
	end
	if first == 169 and second == 254 then
		return true
	end

	return false
end

function module.is_local_network_request(ctx: api.ApiContext?): boolean
	local request = if ctx then ctx.request else nil
	local headers = if request then request.headers else nil
	if type(headers) ~= "table" then
		return false
	end

	local host = parse_host_without_port(headers.host or headers.Host)
	if not host or host == "" then
		return false
	end

	if host == "localhost" or host == "::1" then
		return true
	end
	if is_private_ipv4(host) then
		return true
	end
	if string.sub(host, -6) == ".local" then
		return true
	end

	return false
end

local function query_lookup(query: api.ApiQueryParams?, keys: { string }): string?
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

function module.parse_query_string(query: api.ApiQueryParams?, keys: { string }, default_value: string?): string?
	local value = query_lookup(query, keys)
	if value == nil then
		return default_value
	end

	return module.trim(tostring(value)) or default_value
end

function module.parse_query_int(
	query: api.ApiQueryParams?,
	keys: { string },
	default_value: number?,
	min_value: number?,
	max_value: number?
): number?
	local value = query_lookup(query, keys)
	if value == nil then
		return default_value
	end

	local number: number? = nil
	if type(value) == "number" then
		number = value
	elseif type(value) == "string" then
		number = tonumber(module.trim(value) or "")
	end

	if number == nil then
		return default_value
	end

	local integer = math.floor(number)
	if min_value ~= nil and integer < min_value then
		return default_value
	end
	if max_value ~= nil and integer > max_value then
		return default_value
	end

	return integer
end

function module.normalize_image_format(raw_format: string?): ImageFormat?
	local text = module.trim(raw_format)
	if not text then
		return nil
	end

	local normalized = string.lower(text)
	if normalized == "jpg" or normalized == "png" or normalized == "webp" then
		return normalized
	end

	return nil
end

module.nanoid_to_hex = encoding.nanoid_to_hex
module.hex_to_nanoid = encoding.hex_to_nanoid

local NANOID_LENGTH = 21

function module.encode_jellyfin_item_id(value: number | string?): string?
	if value == nil then
		return nil
	end

	if type(value) == "string" then
		if #value == NANOID_LENGTH then
			return module.nanoid_to_hex(value)
		end
		return nil
	end

	if type(value) == "number" then
		local nanoid = ids.get_id(value)
		if nanoid then
			return module.nanoid_to_hex(nanoid)
		end
		return nil
	end

	return nil
end

local function hex_to_nanoid_safe(value: string?): string?
	local text = module.trim(value)
	if not text then
		return nil
	end

	local normalized = string.gsub(string.lower(text), "-", "")
	if #normalized == ITEM_ID_HEX_LENGTH and string.match(normalized, ITEM_ID_HEX_PATTERN) then
		return module.hex_to_nanoid(normalized)
	end

	return nil
end

function module.decode_jellyfin_item_id(value: string?): number?
	local nanoid = hex_to_nanoid_safe(value)
	if nanoid then
		return ids.get_db_id(nanoid)
	end
	return nil
end

local function batch_decode_hex_ids(hex_values: { string }): { number }
	local nanoids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_hex in hex_values do
		local nanoid = hex_to_nanoid_safe(raw_hex)
		if nanoid and not seen[nanoid] then
			seen[nanoid] = true
			table.insert(nanoids, nanoid)
		end
	end

	if #nanoids == 0 then
		return {}
	end

	local ok, db_id_map = pcall(ids.get_db_ids, nanoids)
	if not ok or type(db_id_map) ~= "table" then
		return {}
	end

	local result: { number } = {}
	for _, nanoid in nanoids do
		local db_id = db_id_map[nanoid]
		if db_id then
			table.insert(result, db_id)
		end
	end
	return result
end

function module.parse_jellyfin_ids_csv(raw_ids: string?): { number }
	if type(raw_ids) ~= "string" then
		return {}
	end

	return batch_decode_hex_ids(split_csv(raw_ids))
end

function module.parse_jellyfin_ids_array(raw_ids: string | { string }?): { number }
	if type(raw_ids) == "string" then
		return module.parse_jellyfin_ids_csv(raw_ids)
	end

	if type(raw_ids) ~= "table" then
		return {}
	end

	return batch_decode_hex_ids(raw_ids)
end

function module.encode_jellyfin_db_ids(db_ids: { number }): { [number]: string? }
	if #db_ids == 0 then
		return {}
	end

	local ok, nanoid_map = pcall(ids.get_ids, db_ids)
	if not ok or type(nanoid_map) ~= "table" then
		return {}
	end

	local result: { [number]: string? } = {}
	for _, db_id in db_ids do
		local nanoid = nanoid_map[db_id]
		if nanoid then
			result[db_id] = module.nanoid_to_hex(nanoid)
		end
	end
	return result
end

function module.normalize_cover_info(raw_cover: RawCoverInfo?): ResolvedCoverInfo?
	if type(raw_cover) ~= "table" then
		return nil
	end

	local path = module.trim(raw_cover.path)
	local hash = module.trim(raw_cover.hash)
	if not path or not hash then
		return nil
	end

	return {
		path = path,
		mime_type = module.trim(raw_cover.mime_type) or "application/octet-stream",
		hash = hash,
		blurhash = module.trim(raw_cover.blurhash),
		release_id = module.encode_jellyfin_item_id(raw_cover.release_id),
	}
end

function module.get_cover_info(item_id: number?): ResolvedCoverInfo?
	if item_id == nil or item_id <= 0 then
		return nil
	end
	local ok, raw_cover = pcall(covers.get, item_id)
	if not ok then
		return nil
	end
	return module.normalize_cover_info(raw_cover)
end

function module.build_query_result<T>(items: { T }, total_count: number, start_index: number): api.JsonValue
	return {
		Items = items,
		TotalRecordCount = total_count,
		StartIndex = start_index,
	}
end

function module.paginate_items<T>(items: { T }, start_index: number?, limit: number?): ({ T }, number, number)
	local total = #items
	local safe_start = start_index or 0
	if safe_start > total then
		safe_start = total
	end

	local finish_exclusive = total
	if limit ~= nil then
		finish_exclusive = math.min(total, safe_start + limit)
	end

	local paged: { T } = {}
	local output_index = 1
	for index = safe_start + 1, finish_exclusive do
		paged[output_index] = items[index]
		output_index += 1
	end

	return paged, total, safe_start
end

function module.extension_from_path(path: string?): string?
	if type(path) ~= "string" then
		return nil
	end

	local filename = string.match(path, "[^/\\]+$") or path
	local ext = string.match(filename, "%.([^.]+)$")
	if not ext then
		return nil
	end

	return string.lower(ext)
end

function module.mime_type_for_extension(extension: string?): string
	if extension == "flac" then
		return "audio/flac"
	end
	if extension == "mp3" then
		return "audio/mpeg"
	end
	if extension == "aac" then
		return "audio/aac"
	end
	if extension == "m4a" or extension == "mp4" then
		return "audio/mp4"
	end
	if extension == "opus" or extension == "ogg" then
		return "audio/ogg"
	end
	if extension == "wav" then
		return "audio/wav"
	end
	if extension == "aif" or extension == "aiff" then
		return "audio/aiff"
	end

	return "application/octet-stream"
end

function module.resolve_entry_for_track(track: { db_id: number? }?): EntryInfo?
	local source = module.resolve_playback_source_for_track(track, true)
	if source and type(source.entry) == "table" then
		return source.entry
	end

	return nil
end

function module.resolve_playback_source_for_track(
	track: { db_id: number? }?,
	include_entry: boolean?
): PlaybackSourceInfo?
	if not track or not track.db_id then
		return nil
	end

	local ok, source_items = pcall(playback_sources.get, track.db_id, include_entry == true)
	if not ok or type(source_items) ~= "table" then
		return nil
	end
	for _, source in source_items do
		if source and type(source) == "table" then
			return source
		end
	end

	return nil
end

return module
