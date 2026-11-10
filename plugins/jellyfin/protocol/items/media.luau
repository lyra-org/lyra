--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local playback_sources = require("@lyra/playback_sources")

local enrichment = require("./enrichment")
local item_ids = require("./identifiers")

type PlaybackSourceInfo = playback_sources.PlaybackSourceInfo
type EntryInfo = playback_sources.EntryInfo

export type MediaStream = {
	Codec: string?,
	Type: string,
	Index: number,
	Channels: number?,
	SampleRate: number?,
	BitDepth: number?,
	IsDefault: boolean,
	IsForced: boolean,
	IsHearingImpaired: boolean,
	IsOriginal: boolean,
	IsInterlaced: boolean,
	IsExternal: boolean,
	IsTextSubtitleStream: boolean,
	SupportsExternalStream: boolean,
	BitRate: number?,
}

export type MediaSource = {
	Protocol: string,
	Id: string?,
	Path: string?,
	Type: string,
	Container: string?,
	Size: number?,
	Name: string,
	IsRemote: boolean,
	RunTimeTicks: number?,
	ReadAtNativeFramerate: boolean,
	IgnoreDts: boolean,
	IgnoreIndex: boolean,
	GenPtsInput: boolean,
	SupportsTranscoding: boolean,
	SupportsDirectStream: boolean,
	SupportsDirectPlay: boolean,
	IsInfiniteStream: boolean,
	RequiresOpening: boolean,
	RequiresClosing: boolean,
	RequiresLooping: boolean,
	SupportsProbing: boolean,
	MediaStreams: { MediaStream },
	Bitrate: number?,
	TranscodingContainer: string?,
	TranscodingUrl: string?,
	TranscodingSubProtocol: string?,
	DefaultAudioStreamIndex: number?,
	HasSegments: boolean,
	ETag: string?,
}

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

-- What an item's media source reports for a file whose format goes by several names: the file's
-- extension, if it is one of them or an audiobook's `m4b`, else the first name. Upstream's item
-- DTOs do the same, without `m4b`. Without the file there is nothing to go by.
function module.item_source_container(source: PlaybackSourceInfo?): string?
	local names = module.probed_container(source)
	local entry = if source then source.entry else nil
	if names == nil or entry == nil or string.find(names, ",", 1, true) == nil then
		return module.jellyfin_container(source)
	end
	local extension = string.match(entry.full_path or entry.name or "", "%.([^%./\\]+)$")
	local lowered = if extension then string.lower(extension) else nil
	local listed = string.split(names, ",")
	if lowered and (lowered == "m4b" or table.find(listed, lowered)) then
		return lowered
	end
	return listed[1]
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

-- A track's media source and streams, its lyrics among them; `container` is what the source reports.
function module.build_media_info(
	track: { duration_ms: number? }?,
	source: PlaybackSourceInfo?,
	item_id: string?,
	fallback_name: string?,
	has_lyrics: boolean?,
	container: string?
): ({ MediaSource }, { MediaStream })
	local entry: EntryInfo? = if source and type(source.entry) == "table" then source.entry else nil
	local media_item_id = item_ids.encode(item_id)
	local codec = module.jellyfin_codec(source)
	local trimmed = module.is_trimmed(source)
	local size = if entry and entry.size and not trimmed then tonumber(entry.size) else nil
	local bitrate = if source then source.bitrate_bps else nil
	local direct_playable = source ~= nil and media_item_id ~= nil and not trimmed

	local media_stream: MediaStream = {
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
	local media_streams = { media_stream }
	if has_lyrics then
		local lyric_stream: MediaStream = {
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
	end

	local media_source: MediaSource = {
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
		MediaStreams = media_streams,
		Bitrate = bitrate,
		DefaultAudioStreamIndex = 0,
		HasSegments = false,
	}
	if entry and entry.hash then
		media_source.ETag = tostring(entry.hash)
	end

	return { media_source }, media_streams
end

function module.resolve_entry_for_track(track: { id: string }?): EntryInfo?
	local source = module.resolve_playback_source_for_track(track, true)
	if source and type(source.entry) == "table" then
		return source.entry
	end

	return nil
end

function module.resolve_playback_source_for_track(track: { id: string }?, include_entry: boolean?): PlaybackSourceInfo?
	if not track then
		return nil
	end

	local ok, source_items = pcall(playback_sources.get, track.id, { include_entry = include_entry == true })
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
function module.resolve_public_playback_source(track: { id: string }?, include_entry: boolean?): PlaybackSourceInfo?
	if not track then
		return nil
	end

	local ok, source = pcall(playback_sources.streamable, track.id, { include_entry = include_entry == true })
	if not ok or type(source) ~= "table" then
		return nil
	end
	return source
end

return module
