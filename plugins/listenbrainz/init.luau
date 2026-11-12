--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local documents = require("@self/documents")
local entities = require("@lyra/entities")
local http = require("@harmony/http")
local listens = require("@lyra/listens")
local serde = require("@harmony/serde")
local json = serde.json
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
type ListenUpdatePayload = listens.ListenUpdatePayload
type JsonValue = serde.JsonValue
type HttpRequestOptions = http.HttpRequestOptions
type TrackProjection = entities.Track
type ReleaseProjection = entities.Release
type ArtistProjection = entities.Artist
type TrackProjectionInfo = entities.TrackProjectionInfo
type ReleaseProjectionInfo = entities.ReleaseProjectionInfo
type ArtistProjectionInfo = entities.ArtistProjectionInfo
type TrackProjectionIncludes = entities.TrackProjectionIncludes
type ExternalIdsByScheme = { [string]: string }

type AdditionalInfo = documents.AdditionalInfo
type TrackMetadata = documents.TrackMetadata

http.set_rate_limit({ domain = "api.listenbrainz.org" })

local function is_non_empty_string(value: string?): boolean
	return type(value) == "string" and value:match("%S") ~= nil
end

local function require_non_empty_string(value: string?): string?
	if is_non_empty_string(value) then
		return value
	end
	return nil
end

local function release_projection(release_id: string): ReleaseProjectionInfo?
	local projection = entities.get({ release_id }, { include = "external_ids" })[release_id]
	return if projection and projection.entity_type == "release" then projection else nil
end

local function artist_projection(artist_id: string): ArtistProjectionInfo?
	local projection = entities.get({ artist_id }, { include = "external_ids" })[artist_id]
	return if projection and projection.entity_type == "artist" then projection else nil
end

local function track_projection(track_id: string): (TrackProjection?, TrackProjectionIncludes?)
	local projection = entities.get({ track_id }, { include = { "artists", "releases", "external_ids" } })[track_id]
	if not projection or projection.entity_type ~= "track" then
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
		local projection = release_projection(release.id)
		local external_ids = if projection then projection.includes.external_ids else nil
		local release_mbid = if external_ids then external_ids["musicbrainz:release"] else nil
		local release_group_mbid = if external_ids then external_ids["musicbrainz:release-group"] else nil

		return release.release_title, release_mbid, release_group_mbid
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
		local projection = artist_projection(artist.id)
		local external_ids = if projection then projection.includes.external_ids else nil
		local artist_mbid = if external_ids then external_ids["musicbrainz:artist"] else nil
		if artist_mbid ~= nil and not seen[artist_mbid] then
			seen[artist_mbid] = true
			table.insert(mbids, artist_mbid)
		end
	end

	return mbids
end

local function build_additional_info(
	track: TrackProjection?,
	external_ids: ExternalIdsByScheme?,
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

	additional_info.recording_mbid = if external_ids then external_ids["musicbrainz:recording"] else nil
	additional_info.release_mbid = release_mbid
	additional_info.release_group_mbid = release_group_mbid

	local artist_mbids = collect_artist_mbids(artists_list)
	if #artist_mbids > 0 then
		additional_info.artist_mbids = artist_mbids
	end

	return additional_info
end

local function build_track_metadata(track_id: string): TrackMetadata?
	local track, includes = track_projection(track_id)
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

local function submit_listens(token: string, document: JsonValue)
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
		warn(`submit to ListenBrainz failed: status={response.status_code} message={response.status_message}`)
		if response.body and response.body ~= "" then
			warn(`response body from ListenBrainz: {response.body}`)
		end
	end
end

local function user_token(user_id: string): string?
	local ok, settings = pcall(user_config.get, user_config, user_id)
	if not ok then
		return nil
	end
	return require_non_empty_string(settings.token :: string?)
end

playbacks.on_update(function(update: PlaybackUpdatePayload)
	if update.event ~= "started" then
		return
	end
	local token = user_token(update.user_id)
	if not token then
		return
	end
	local track_metadata = build_track_metadata(update.track_id)
	if not track_metadata then
		warn(`skipping ListenBrainz playing_now for track_id={update.track_id}: missing artist_name or track_name`)
		return
	end
	submit_listens(token, documents.playing_now(track_metadata))
end)

listens.on_update(function(update: ListenUpdatePayload)
	if update.event ~= "recorded" then
		return
	end
	local token = user_token(update.user_id)
	if not token then
		return
	end
	local metadata_by_track: { [string]: TrackMetadata | false } = {}
	local submissions: { documents.Submission } = {}
	for _, listen in update.listens do
		local track_metadata = metadata_by_track[listen.track_id]
		if track_metadata == nil then
			track_metadata = build_track_metadata(listen.track_id) or false
			metadata_by_track[listen.track_id] = track_metadata
			if not track_metadata then
				warn(`skipping ListenBrainz listens of track_id={listen.track_id}: missing artist_name or track_name`)
			end
		end
		if track_metadata then
			table.insert(submissions, { listen = listen, track_metadata = track_metadata })
		end
	end
	for _, document in documents.listens(submissions) do
		submit_listens(token, document)
	end
end)
