--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local playback_sources = require("@lyra/playback_sources")

type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo

local module = {}

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
