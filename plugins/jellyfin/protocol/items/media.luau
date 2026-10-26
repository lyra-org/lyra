--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local playback_sources = require("@lyra/playback_sources")

type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo

local module = {}

-- Jellyfin names containers after FFmpeg's demuxers, and codecs as Lyra already does.
function module.jellyfin_container(source: PlaybackSourceInfo?): string?
	local container = if source then source.container else nil
	if container == "wavpack" then
		return "wv"
	elseif container == "mpc" and source and source.codec == "musepack8" then
		return "mpc8"
	end
	return container
end

-- Upstream's playback sources keep every name FFmpeg's demuxer goes by, where its item DTOs
-- collapse them to the file's extension.
function module.probed_container(source: PlaybackSourceInfo?): string?
	if source and source.container == "m4a" then
		return "mov,mp4,m4a,3gp,3g2,mj2"
	end
	return module.jellyfin_container(source)
end

-- A source cut from a longer file, as a cue sheet's tracks are. Upstream has no such source; the
-- file can't be handed over as the track, and its size isn't the track's, so clients are told to
-- transcode, as upstream tells them for a source it can't direct play.
function module.is_trimmed(source: PlaybackSourceInfo?): boolean
	return source ~= nil and source.is_virtual
end

function module.jellyfin_codec(source: PlaybackSourceInfo?): string?
	return if source then source.codec else nil
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

-- Reads as the system for public streaming routes, which must serve nothing from it but the audio.
function module.resolve_public_playback_source(track: { db_id: number? }?, include_entry: boolean?): PlaybackSourceInfo?
	if not track or not track.db_id then
		return nil
	end

	local ok, source = pcall(playback_sources.streamable, track.db_id, include_entry == true)
	if not ok or type(source) ~= "table" then
		return nil
	end
	return source
end

return module
