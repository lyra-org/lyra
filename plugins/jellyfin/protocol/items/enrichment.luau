--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local playlists = require("@lyra/playlists")
local tracks = require("@lyra/tracks")

type Track = tracks.Track

local module = {}

local function append_unique_id(id: number?, output: { number }, seen: { [number]: boolean })
	if id ~= nil and id > 0 and not seen[id] then
		seen[id] = true
		table.insert(output, id)
	end
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

return module
