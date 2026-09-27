--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local listens = require("@lyra/listens")
local playlists = require("@lyra/playlists")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

type Release = releases.Release
type Track = tracks.Track

export type PlayStats = {
	play_count: number,
	last_played: number?,
}
type KeyedItem = {
	_type: string?,
	_raw: { db_id: number?, duration_ms: number? }?,
	_play_count: number?,
	_last_played: number?,
	_release_keys_loaded: boolean?,
	Album: string?,
	PremiereDate: string?,
	ProductionYear: number?,
	RunTimeTicks: number?,
	ChildCount: number?,
}

local RELEASE_SORT_TOKENS = {
	album = true,
	release = true,
	premieredate = true,
	productionyear = true,
}

local PLAY_STATS_SORT_TOKENS = {
	playcount = true,
	dateplayed = true,
}

local RUNTIME_SORT_TOKENS = {
	runtime = true,
	childcount = true,
}

local module = {}

local function append_unique_id(id: number?, output: { number }, seen: { [number]: boolean })
	if id ~= nil and id > 0 and not seen[id] then
		seen[id] = true
		table.insert(output, id)
	end
end

local function item_db_id(item: KeyedItem): number?
	local raw = item._raw
	local db_id = if raw then raw.db_id else nil
	return if db_id and db_id > 0 then db_id else nil
end

function module.ms_to_ticks(duration_ms: number | string?): number?
	local value = tonumber(duration_ms)
	if not value then
		return nil
	end

	return math.floor(value * 10000)
end

function module.sum_runtime_ticks(track_items: { Track }?): number
	local total_ticks = 0
	for _, track: Track in track_items or {} do
		total_ticks += module.ms_to_ticks(track.duration_ms) or 0
	end
	return total_ticks
end

function module.release_year(release_date: string?): number?
	if not release_date or string.len(release_date) < 4 then
		return nil
	end
	return tonumber(string.sub(release_date, 1, 4))
end

function module.release_date_to_premiere_date(release_date: string?): string?
	local y = module.release_year(release_date)
	if not y or not release_date then
		return nil
	end
	local month = tonumber(string.sub(release_date, 6, 7)) or 1
	local day = tonumber(string.sub(release_date, 9, 10)) or 1
	return string.format("%04d-%02d-%02dT00:00:00.0000000Z", math.floor(y), math.floor(month), math.floor(day))
end

-- A release date from a different year than the track's own falls back to January 1st.
function module.track_premiere_date(track_year: number?, release_date: string?): string?
	if track_year == nil or module.release_year(release_date) == track_year then
		return module.release_date_to_premiere_date(release_date)
	end
	return string.format("%04d-01-01T00:00:00.0000000Z", math.floor(track_year))
end

-- Lowest release id, matching the default pick for track items.
function module.primary_release(release_list: { Release }?): Release?
	local primary: Release? = nil
	for _, release: Release in release_list or {} do
		local release_id = release.db_id
		if release_id ~= nil and release_id > 0 and (primary == nil or release_id < (primary.db_id :: number)) then
			primary = release
		end
	end
	return primary
end

function module.primary_releases_by_track(track_ids: { number }): { [number]: Release }
	local primary_releases: { [number]: Release } = {}
	if #track_ids == 0 then
		return primary_releases
	end

	for track_id, release_list in releases.by_track(track_ids) do
		local release = module.primary_release(release_list)
		if release then
			primary_releases[track_id] = release
		end
	end
	return primary_releases
end

-- Releases and artists aggregate the stats of their tracks.
function module.play_stats(track_ids: { number }, owner_ids: { number }): { [number]: PlayStats }
	local stats_by_id: { [number]: PlayStats } = {}
	local tracks_by_owner: { [number]: { Track } } = if #owner_ids > 0 then tracks.by_release(owner_ids) else {}
	local lookup_ids: { number } = {}
	local lookup_seen: { [number]: boolean } = {}
	for _, track_id in track_ids do
		append_unique_id(track_id, lookup_ids, lookup_seen)
	end
	for _, owner_tracks in tracks_by_owner do
		for _, track: Track in owner_tracks do
			append_unique_id(track.db_id, lookup_ids, lookup_seen)
		end
	end
	if #lookup_ids == 0 then
		return stats_by_id
	end

	local stats = listens.get_stats(lookup_ids)
	local counts = stats.counts or {}
	local last_played = stats.last_played or {}
	local function track_stats(track_id: number): PlayStats
		local played_at = tonumber(last_played[track_id])
		return {
			play_count = tonumber(counts[track_id]) or 0,
			last_played = if played_at and played_at > 0 then played_at else nil,
		}
	end

	for _, track_id in track_ids do
		stats_by_id[track_id] = track_stats(track_id)
	end
	for _, owner_id in owner_ids do
		local aggregate: PlayStats = { play_count = 0, last_played = nil }
		for _, track: Track in tracks_by_owner[owner_id] or {} do
			if track.db_id then
				local entry = track_stats(track.db_id)
				aggregate.play_count += entry.play_count
				local latest = aggregate.last_played
				if entry.last_played and (latest == nil or entry.last_played > latest) then
					aggregate.last_played = entry.last_played
				end
			end
		end
		stats_by_id[owner_id] = aggregate
	end
	return stats_by_id
end

-- Duplicate entries count once per occurrence.
function module.playlist_runtime_ticks(playlist_db_ids: { number }): { [number]: number }
	local runtime_ticks: { [number]: number } = {}
	if #playlist_db_ids == 0 then
		return runtime_ticks
	end

	local links_by_playlist = playlists.get_tracks_many(playlist_db_ids)
	local track_ids: { number } = {}
	local track_seen: { [number]: boolean } = {}
	for _, links in links_by_playlist do
		for _, link in links do
			append_unique_id(link.track_id, track_ids, track_seen)
		end
	end
	local tracks_by_id: { [number]: Track } = if #track_ids > 0 then tracks.get(track_ids) else {}

	for _, playlist_db_id in playlist_db_ids do
		local total_ticks = 0
		for _, link in links_by_playlist[playlist_db_id] or {} do
			local track = tracks_by_id[link.track_id]
			total_ticks += module.ms_to_ticks(if track then track.duration_ms else nil) or 0
		end
		runtime_ticks[playlist_db_id] = total_ticks
	end
	return runtime_ticks
end

-- Track stubs carry no release fields until these are loaded.
local function load_release_keys(items: { KeyedItem })
	local track_ids: { number } = {}
	local track_seen: { [number]: boolean } = {}
	for _, item in items do
		if item._type == "track" and not item._release_keys_loaded then
			append_unique_id(item_db_id(item), track_ids, track_seen)
		end
	end
	if #track_ids == 0 then
		return
	end

	local releases_by_track = module.primary_releases_by_track(track_ids)
	for _, item in items do
		local db_id = item_db_id(item)
		if db_id == nil or not track_seen[db_id] or item._release_keys_loaded then
			continue
		end
		item._release_keys_loaded = true
		local release = releases_by_track[db_id]
		if release then
			item.Album = release.release_title
			item.PremiereDate = module.release_date_to_premiere_date(release.release_date)
			item.ProductionYear = module.release_year(release.release_date)
		end
	end
end

local function load_play_stats(items: { KeyedItem })
	local track_ids: { number } = {}
	local owner_ids: { number } = {}
	local seen: { [number]: boolean } = {}
	for _, item in items do
		local kind = item._type
		if item._play_count ~= nil then
			continue
		elseif kind == "track" then
			append_unique_id(item_db_id(item), track_ids, seen)
		elseif kind == "release" or kind == "artist" then
			append_unique_id(item_db_id(item), owner_ids, seen)
		end
	end
	if #track_ids == 0 and #owner_ids == 0 then
		return
	end

	local stats_by_id = module.play_stats(track_ids, owner_ids)
	for _, item in items do
		local db_id = item_db_id(item)
		local stats = if db_id then stats_by_id[db_id] else nil
		if stats and item._play_count == nil then
			item._play_count = stats.play_count
			item._last_played = stats.last_played
		end
	end
end

local function load_runtime_keys(items: { KeyedItem })
	local owner_ids: { number } = {}
	local owner_seen: { [number]: boolean } = {}
	local playlist_ids: { number } = {}
	local playlist_seen: { [number]: boolean } = {}
	for _, item in items do
		local kind = item._type
		local raw = item._raw
		if item.RunTimeTicks ~= nil then
			continue
		elseif kind == "track" then
			item.RunTimeTicks = module.ms_to_ticks(if raw then raw.duration_ms else nil)
		elseif kind == "release" or kind == "artist" then
			append_unique_id(item_db_id(item), owner_ids, owner_seen)
		elseif kind == "playlist" then
			append_unique_id(item_db_id(item), playlist_ids, playlist_seen)
		end
	end

	local tracks_by_owner: { [number]: { Track } } = if #owner_ids > 0 then tracks.by_release(owner_ids) else {}
	local playlist_runtimes = module.playlist_runtime_ticks(playlist_ids)
	for _, item in items do
		local db_id = item_db_id(item)
		if db_id == nil or item.RunTimeTicks ~= nil then
			continue
		end
		if owner_seen[db_id] then
			local owner_tracks = tracks_by_owner[db_id] or {}
			item.RunTimeTicks = module.sum_runtime_ticks(owner_tracks)
			item.ChildCount = #owner_tracks
		elseif playlist_seen[db_id] then
			item.RunTimeTicks = playlist_runtimes[db_id]
		end
	end
end

function module.load_sort_keys<T>(items: { T }, sort_tokens: { string })
	local keyed_items = (items :: any) :: { KeyedItem }
	local needs_release_keys = false
	local needs_play_stats = false
	local needs_runtime_keys = false
	for _, token in sort_tokens do
		needs_release_keys = needs_release_keys or RELEASE_SORT_TOKENS[token] == true
		needs_play_stats = needs_play_stats or PLAY_STATS_SORT_TOKENS[token] == true
		needs_runtime_keys = needs_runtime_keys or RUNTIME_SORT_TOKENS[token] == true
	end
	if needs_release_keys then
		load_release_keys(keyed_items)
	end
	if needs_play_stats then
		load_play_stats(keyed_items)
	end
	if needs_runtime_keys then
		load_runtime_keys(keyed_items)
	end
end

-- Items without a year never match, as in Jellyfin.
function module.filter_years<T>(items: { T }, years: { number }?): { T }
	if years == nil or #years == 0 then
		return items
	end

	local allowed: { [number]: boolean } = {}
	for _, year in years do
		allowed[year] = true
	end

	load_release_keys((items :: any) :: { KeyedItem })
	local filtered: { T } = {}
	for _, item in items do
		local year = ((item :: any) :: KeyedItem).ProductionYear
		if year ~= nil and allowed[year] then
			table.insert(filtered, item)
		end
	end
	return filtered
end

function module.filter_played<T>(items: { T }, is_played: boolean?): { T }
	if is_played == nil then
		return items
	end

	load_play_stats((items :: any) :: { KeyedItem })
	local filtered: { T } = {}
	for _, item in items do
		if (((item :: any) :: KeyedItem)._play_count or 0) > 0 == is_played then
			table.insert(filtered, item)
		end
	end
	return filtered
end

return module
