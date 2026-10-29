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
type EntityQueryRequest = entities.EntityQueryRequest
type IdentifiersByScheme = { [string]: string }

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

local function query_track_projection(track_public_id: string): (TrackProjection?, TrackProjectionIncludes?)
	local request: EntityQueryRequest = {
		id = track_public_id,
		include = { "artists", "releases", "identifiers" },
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
			local release_projection = query_release_projection(release_db_id, "identifiers")
			local identifiers = if release_projection then release_projection.includes.identifiers else nil
			local release_mbid = if identifiers then identifiers["musicbrainz:release"] else nil
			local release_group_mbid = if identifiers then identifiers["musicbrainz:release-group"] else nil

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
			local artist_projection = query_artist_projection(artist_id, "identifiers")
			local identifiers = if artist_projection then artist_projection.includes.identifiers else nil
			local artist_mbid = if identifiers then identifiers["musicbrainz:artist"] else nil
			if artist_mbid ~= nil and not seen[artist_mbid] then
				seen[artist_mbid] = true
				table.insert(mbids, artist_mbid)
			end
		end
	end

	return mbids
end

local function build_additional_info(
	track: TrackProjection?,
	identifiers: IdentifiersByScheme?,
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

	additional_info.recording_mbid = if identifiers then identifiers["musicbrainz:recording"] else nil
	additional_info.release_mbid = release_mbid
	additional_info.release_group_mbid = release_group_mbid

	local artist_mbids = collect_artist_mbids(artists_list)
	if #artist_mbids > 0 then
		additional_info.artist_mbids = artist_mbids
	end

	return additional_info
end

local function build_track_metadata(track_public_id: string): TrackMetadata?
	local track, includes = query_track_projection(track_public_id)
	local artists_list = if includes then includes.artists else nil
	local releases_list = if includes then includes.releases else nil
	local identifiers = if includes then includes.identifiers else nil
	local artist_name = require_non_empty_string(resolve_artist_name(artists_list))
	local track_name = require_non_empty_string(if track then track.track_title else nil)
	local release_name, release_mbid, release_group_mbid = resolve_release_metadata(releases_list)

	if artist_name == nil or track_name == nil then
		return nil
	end

	local metadata: TrackMetadata = {
		artist_name = artist_name,
		track_name = track_name,
		additional_info = build_additional_info(track, identifiers, artists_list, release_mbid, release_group_mbid),
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

local function user_token(user_public_id: string): string?
	local ok, settings = pcall(user_config.get, user_config, user_public_id)
	if not ok then
		return nil
	end
	return require_non_empty_string(settings.token :: string?)
end

playbacks.on_update(function(update: PlaybackUpdatePayload)
	if update.event ~= "started" then
		return
	end
	local token = user_token(update.user_public_id)
	if not token then
		return
	end
	local track_metadata = build_track_metadata(update.track_public_id)
	if not track_metadata then
		warn(
			`skipping ListenBrainz playing_now for track_public_id={update.track_public_id}: missing artist_name or track_name`
		)
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
