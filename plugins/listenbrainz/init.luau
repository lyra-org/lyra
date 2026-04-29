--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local entities = require("@lyra/entities")
local http = require("@harmony/http")
local json = require("@harmony/json")
local metadata = require("@lyra/metadata")
local playbacks = require("@lyra/playback_sessions")
local plugins = require("@lyra/plugins")

local user_config = plugins.declare_user_settings(function(builder)
	builder:group("authentication", "Authentication")
	builder:string("token", {
		label = "API Token",
		description = "Your personal ListenBrainz user token.",
	})
end)

local LISTENBRAINZ_SUBMIT_URL = "https://api.listenbrainz.org/1/submit-listens"

type PlaybackUpdatePayload = playbacks.PlaybackUpdatePayload
type JsonValue = json.JsonValue
type HttpRequestOptions = http.HttpRequestOptions
type TrackProjection = entities.Track
type ReleaseProjection = entities.Release
type ArtistProjection = entities.Artist
type TrackProjectionInfo = entities.TrackProjectionInfo
type ReleaseProjectionInfo = entities.ReleaseProjectionInfo
type ArtistProjectionInfo = entities.ArtistProjectionInfo
type TrackProjectionIncludes = entities.TrackProjectionIncludes
type EntityQueryRequest = entities.EntityQueryRequest
type ExternalIdsByProvider = metadata.ExternalIdsByProvider

type AdditionalInfo = {
	submission_client: string,
	submission_client_version: string,
	duration_ms: number?,
	tracknumber: string?,
	discnumber: string?,
	recording_mbid: string?,
	release_mbid: string?,
	release_group_mbid: string?,
	artist_mbids: { string }?,
}

type TrackMetadata = {
	artist_name: string,
	track_name: string,
	release_name: string?,
	additional_info: AdditionalInfo,
}

http.set_rate_limit({ domain = "api.listenbrainz.org" })

local started_at_sec_by_playback_session_id: { [number]: number } = {}
local single_submitted_by_playback_session_id: { [number]: boolean } = {}

local function unix_seconds_from_ms(ms: number?): number
	return math.floor((ms or 0) / 1000)
end

local function is_non_empty_string(value: string?): boolean
	return type(value) == "string" and value:match("%S") ~= nil
end

local function require_non_empty_string(value: string?): string?
	if is_non_empty_string(value) then
		return value
	end
	return nil
end

local provider_external_ids = metadata.ids.for_provider

local function query_release_projection(entity_id: number, include: string | { string }): ReleaseProjectionInfo?
	local request: EntityQueryRequest = {
		id = entity_id,
		include = include,
	}
	local ok, projection = pcall(entities.query_release, request)
	if not ok or type(projection) ~= "table" then
		return nil
	end
	return projection
end

local function query_artist_projection(entity_id: number, include: string | { string }): ArtistProjectionInfo?
	local request: EntityQueryRequest = {
		id = entity_id,
		include = include,
	}
	local ok, projection = pcall(entities.query_artist, request)
	if not ok or type(projection) ~= "table" then
		return nil
	end
	return projection
end

local function query_track_projection(track_id: number): (TrackProjection?, TrackProjectionIncludes?)
	local request: EntityQueryRequest = {
		id = track_id,
		include = { "artists", "releases", "external_ids" },
	}
	local ok, projection = pcall(entities.query_track, request)
	if not ok or type(projection) ~= "table" then
		return nil, nil
	end
	return projection.entity, projection.includes
end

local function resolve_artist_name(artists_list: { ArtistProjection }?): string?
	if type(artists_list) ~= "table" then
		return nil
	end

	for _, artist in artists_list do
		if is_non_empty_string(artist.artist_name) then
			return artist.artist_name
		end
	end

	return nil
end

local function resolve_release_metadata(releases_list: { ReleaseProjection }?): (string?, string?, string?)
	if type(releases_list) ~= "table" then
		return nil, nil, nil
	end

	for _, release in releases_list do
		local release_db_id = release.db_id
		local release_name = release.release_title
		if release_db_id ~= nil then
			local release_projection = query_release_projection(release_db_id, "external_ids")
			local release_external_ids = if release_projection then release_projection.includes.external_ids else nil
			local musicbrainz_ids = provider_external_ids(release_external_ids, "musicbrainz")
			local release_mbid = if musicbrainz_ids ~= nil then musicbrainz_ids.release_id else nil
			local release_group_mbid = if musicbrainz_ids ~= nil then musicbrainz_ids.release_group_id else nil

			return release_name, release_mbid, release_group_mbid
		end
	end

	return nil, nil, nil
end

local function collect_artist_mbids(artists_list: { ArtistProjection }?): { string }
	local mbids: { string } = {}
	local seen: { [string]: boolean } = {}
	if type(artists_list) ~= "table" then
		return mbids
	end

	for _, artist in artists_list do
		local artist_id = artist.db_id
		if artist_id ~= nil then
			local artist_projection = query_artist_projection(artist_id, "external_ids")
			local artist_external_ids = if artist_projection then artist_projection.includes.external_ids else nil
			local musicbrainz_ids = provider_external_ids(artist_external_ids, "musicbrainz")
			local artist_mbid = if musicbrainz_ids ~= nil then musicbrainz_ids.artist_id else nil
			local resolved_artist_mbid = require_non_empty_string(artist_mbid)
			if resolved_artist_mbid ~= nil and not seen[resolved_artist_mbid] then
				seen[resolved_artist_mbid] = true
				table.insert(mbids, resolved_artist_mbid)
			end
		end
	end

	return mbids
end

local function build_additional_info(
	track: TrackProjection?,
	external_ids: ExternalIdsByProvider?,
	artists_list: { ArtistProjection }?,
	release_mbid: string?,
	release_group_mbid: string?
): AdditionalInfo
	local additional_info: AdditionalInfo = {
		submission_client = "Lyra ListenBrainz Plugin",
		submission_client_version = "0.0.1",
	}

	if track and track.duration_ms then
		additional_info.duration_ms = track.duration_ms
	end

	if track and track.track then
		additional_info.tracknumber = tostring(track.track)
	end

	if track and track.disc then
		additional_info.discnumber = tostring(track.disc)
	end

	local musicbrainz_ids = provider_external_ids(external_ids, "musicbrainz")
	if musicbrainz_ids ~= nil and is_non_empty_string(musicbrainz_ids.recording_id) then
		additional_info.recording_mbid = musicbrainz_ids.recording_id
	end

	if is_non_empty_string(release_mbid) then
		additional_info.release_mbid = release_mbid
	end

	if is_non_empty_string(release_group_mbid) then
		additional_info.release_group_mbid = release_group_mbid
	end

	local artist_mbids = collect_artist_mbids(artists_list)
	if #artist_mbids > 0 then
		additional_info.artist_mbids = artist_mbids
	end

	return additional_info
end

local function build_track_metadata(update: PlaybackUpdatePayload): TrackMetadata?
	local track, includes = query_track_projection(update.track_id)
	local artists_list = if includes then includes.artists else nil
	local releases_list = if includes then includes.releases else nil
	local external_ids = if includes then includes.external_ids else nil
	local artist_name = require_non_empty_string(resolve_artist_name(artists_list))
	local track_name = require_non_empty_string(if track then track.track_title else nil)
	local release_name, release_mbid, release_group_mbid = resolve_release_metadata(releases_list)

	if artist_name == nil or track_name == nil then
		return nil
	end

	local metadata: TrackMetadata = {
		artist_name = artist_name,
		track_name = track_name,
		additional_info = build_additional_info(track, external_ids, artists_list, release_mbid, release_group_mbid),
	}

	if is_non_empty_string(release_name) then
		metadata.release_name = release_name
	end

	return metadata
end

local function submit_listens(token: string, document: JsonValue): boolean
	local request: HttpRequestOptions = {
		url = LISTENBRAINZ_SUBMIT_URL,
		method = http.HttpMethod.Post,
		headers = {
			Authorization = `Token {token}`,
			["Content-Type"] = "application/json",
			Accept = "application/json",
		},
		body = json.encode(document),
	}
	local response = http.request(request)

	local status_code = tonumber(response.status_code) or 0
	if not response.success or status_code < 200 or status_code >= 300 then
		print(`submit to ListenBrainz failed: status={response.status_code} message={response.status_message}`)
		if response.body and response.body ~= "" then
			print(`response body from ListenBrainz: {response.body}`)
		end

		return false
	end

	return true
end

local function submit_playing_now(token: string, update: PlaybackUpdatePayload): boolean
	local track_metadata = build_track_metadata(update)
	if not track_metadata then
		print(`skipping ListenBrainz playing_now for track_id={update.track_id}: missing artist_name or track_name`)
		return false
	end

	return submit_listens(token, {
		listen_type = "playing_now",
		payload = {
			{
				track_metadata = track_metadata,
			},
		},
	})
end

local function submit_single_listen(token: string, update: PlaybackUpdatePayload): boolean
	local track_metadata = build_track_metadata(update)
	if not track_metadata then
		print(`skipping ListenBrainz single for track_id={update.track_id}: missing artist_name or track_name`)
		return false
	end

	local listened_at = started_at_sec_by_playback_session_id[update.playback_session_id]
	if not listened_at then
		listened_at = unix_seconds_from_ms(update.updated_at_ms)
	end

	return submit_listens(token, {
		listen_type = "single",
		payload = {
			{
				listened_at = listened_at,
				track_metadata = track_metadata,
			},
		},
	})
end

playbacks.on_update(function(update)
	local settings = user_config:get(update.user_id)
	local token = settings.token :: string?
	if not token or token == "" then
		return
	end

	if update.event == "started" then
		started_at_sec_by_playback_session_id[update.playback_session_id] = unix_seconds_from_ms(update.updated_at_ms)
		single_submitted_by_playback_session_id[update.playback_session_id] = nil
		submit_playing_now(token, update)
		return
	end

	if update.event == "evicted" then
		started_at_sec_by_playback_session_id[update.playback_session_id] = nil
		single_submitted_by_playback_session_id[update.playback_session_id] = nil
		return
	end

	if update.event == "stopped" or update.event == "completed" then
		if single_submitted_by_playback_session_id[update.playback_session_id] then
			return
		end

		local qualifies_single_listen = update.qualifies_single_listen == true
		local is_completed = update.event == "completed"
		if not is_completed and not qualifies_single_listen then
			print(
				`skipping ListenBrainz single for playback_session_id={update.playback_session_id}: insufficient playback activity`
			)
			started_at_sec_by_playback_session_id[update.playback_session_id] = nil
			return
		end

		if submit_single_listen(token, update) then
			single_submitted_by_playback_session_id[update.playback_session_id] = true
			started_at_sec_by_playback_session_id[update.playback_session_id] = nil
		end
	end
end)
