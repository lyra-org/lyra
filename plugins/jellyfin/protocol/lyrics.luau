--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local lyra_lyrics = require("@lyra/lyrics")

local base = require("./items/common")
local catalog = require("./items/catalog")
local session = require("./session")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type LyricsInfo = lyra_lyrics.LyricsInfo
type LyricLineInfo = lyra_lyrics.LyricLineInfo
type LyricWordInfo = lyra_lyrics.LyricWordInfo

local TICKS_PER_MS = 10000

local module = {}

local function ms_to_ticks(value: number?): number?
	if value == nil then
		return nil
	end
	return math.floor(value * TICKS_PER_MS)
end

function module.resolve_audio_item(ctx: ApiContext): (number?, catalog.ItemInfo?, ApiResponse?)
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return nil, nil, session.unauthorized()
	end
	if not session.require_matching_user_scope(ctx, principal) then
		return nil, nil, session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local raw_item_id = if params then params.itemId else nil
	if not raw_item_id then
		return nil, nil, base.bad_request("missing item id")
	end

	local track_id = base.decode_jellyfin_item_id(raw_item_id)
	local item = catalog.find_item_by_id(track_id, principal.user_id)
	if not item or item.Type ~= "Audio" then
		return nil, nil, base.not_found("item not found")
	end

	return track_id, item, nil
end

local function lyric_cues_to_json(words: { LyricWordInfo }): { JsonValue }?
	if #words == 0 then
		return nil
	end

	local cues: { JsonValue } = {}
	for index, word in words do
		local next_word = words[index + 1]
		local cue: JsonObject = {
			Position = word.char_start,
			EndPosition = word.char_end,
			Start = ms_to_ticks(word.ts_ms) or 0,
		}
		if next_word then
			cue.End = ms_to_ticks(next_word.ts_ms)
		end
		table.insert(cues, cue)
	end
	return cues
end

local function synced_lyric_lines_to_json(lines: { LyricLineInfo }): ({ JsonValue }, boolean)
	local output: { JsonValue } = {}
	local is_synced = false
	for _, line in lines do
		if line.ts_ms > 0 then
			is_synced = true
		end
		local row: JsonObject = {
			Text = line.text,
			Start = ms_to_ticks(line.ts_ms),
		}
		local cues = lyric_cues_to_json(line.words)
		if cues then
			row.Cues = cues
		end
		table.insert(output, row)
	end
	return output, is_synced
end

local function plain_lyric_lines_to_json(text: string): { JsonValue }
	local output: { JsonValue } = {}
	local normalized = string.gsub(string.gsub(text, "\r\n", "\n"), "\r", "\n")
	if #normalized == 0 then
		return output
	end
	for line in string.gmatch(normalized .. "\n", "(.-)\n") do
		local row: JsonObject = {
			Text = line,
		}
		table.insert(output, row)
	end
	return output
end

function module.to_jellyfin_lyric_dto(item: catalog.ItemInfo, lyric_info: LyricsInfo): JsonObject
	local line_count = #lyric_info.lines
	local lines, is_synced = synced_lyric_lines_to_json(lyric_info.lines)
	if line_count == 0 then
		lines = plain_lyric_lines_to_json(lyric_info.plain_text)
	end

	return {
		Metadata = {
			Artist = item.Artist,
			Album = item.Album,
			Title = item.Name,
			Length = item.RunTimeTicks,
			IsSynced = is_synced,
		},
		Lyrics = lines,
	}
end

local function lyrics_content_type_from_file_name(file_name: string?): string?
	local trimmed = base.trim(file_name)
	if not trimmed then
		return nil
	end

	local extension = string.match(trimmed, "%.([^%.]*)$")
	if
		not extension
		or extension == ""
		or string.find(extension, "/", 1, true)
		or string.find(extension, "\\", 1, true)
	then
		return nil
	end
	local lower = string.lower(extension)
	if lower == "lrc" or lower == "elrc" then
		return "application/lrc"
	end
	if lower == "txt" then
		return "text/plain"
	end
	return nil
end

function module.resolve_upload_content_type(ctx: ApiContext): (string?, ApiResponse?)
	local query = if ctx and ctx.request then ctx.request.query else nil
	local file_name = base.parse_query_string(query, { "fileName", "FileName", "filename" }, nil)
	if not base.trim(file_name) then
		return nil, base.bad_request("fileName is required")
	end

	local content_type = lyrics_content_type_from_file_name(file_name)
	if not content_type then
		return nil, base.bad_request("Extension is required on filename")
	end
	return content_type, nil
end

return module
