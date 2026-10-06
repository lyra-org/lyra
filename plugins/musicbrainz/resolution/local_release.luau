--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local libraries = require("@lyra/libraries")
local metadata = require("@lyra/metadata")
local serde = require("@harmony/serde")

local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type JsonValue = serde.JsonValue
type LookupHints = text_helpers.LookupHints
type LookupArtist = text_helpers.LookupHintArtist
type LookupTrack = text_helpers.LookupHintTrack
type LookupHintLibrary = text_helpers.LookupHintLibrary
type LookupContext = text_helpers.LookupHintReleaseContext
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type EditionHints = track_helpers.EditionHints

export type ArtistIdSet = { [string]: boolean }

export type ReleaseIds = {
	release_id: string?,
	release_group_id: string?,
	[string]: JsonValue,
}

export type ReleaseContext = {
	db_id: number?,
	lookup_hints: LookupHints?,
	release_title: string?,
	artists: { LocalArtist }?,
	tracks: { LocalTrack },
	library: LookupHintLibrary?,
	library_id: number?,
	media_formats: { string }?,
	barcode: string?,
	ids: ReleaseIds?,
	external_ids: metadata.ExternalIdsByProvider?,
	manual_ids: metadata.ExternalIdsByProvider?,
	[string]: JsonValue,
}

-- What is known about a local release before matching it: its context, the
-- inputs derived from it once per refresh, and evidence that candidate
-- producers add along the way (the artist search's results, recordings known to
-- be the local audio). `rejected_release_ids` remembers releases acceptance
-- rejected for this evidence; deriving new evidence starts it over.
export type LocalRelease = {
	context: ReleaseContext,
	artist_name: string?,
	release_title: string?,
	track_count: number,
	preferred_language: string?,
	preferred_country: string?,
	compilation_release: boolean,
	edition_hints: EditionHints?,
	artist_ids: ArtistIdSet?,
	-- The artist search found no artist named like the local one, so a release
	-- crediting another name may still be the local artist's.
	artist_missing_from_musicbrainz: boolean,
	-- Recordings known to be the local audio: from an AcoustID fingerprint, or
	-- assigned to local tracks by an edition acceptance already passed.
	verified_recording_ids: { [string]: boolean }?,
	rejected_release_ids: { [string]: boolean },
}

local trim_text = text_helpers.trim_text
local is_blank_text = text_helpers.is_blank_text
local is_placeholder_text = text_helpers.is_placeholder_text
local is_generic_artist = text_helpers.is_generic_artist
local infer_lookup_hints = text_helpers.infer_lookup_hints
local artist_name_from_tracks = track_helpers.artist_name_from_tracks
local is_compilation = track_helpers.is_compilation

local module = {}

local function normalize_preferred_language(language_code: string?): string?
	local normalized = string.lower(trim_text(language_code or ""))
	if normalized == "" then
		return nil
	end
	return normalized
end

local function normalize_preferred_country(country: string?): string?
	local normalized = string.upper(trim_text(country or ""))
	if normalized == "" then
		return nil
	end
	return normalized
end

local function release_context_track_count(release_context: ReleaseContext?): number
	local track_list = if release_context then release_context.tracks else nil
	local track_count = if track_list then #track_list else 0
	if track_list then
		for _, track: LocalTrack in track_list do
			if
				track.track_total
				and track.track_total > track_count
				and track_count >= math.ceil(track.track_total * 0.5)
			then
				track_count = track.track_total
				break
			end
		end
	end
	return track_count
end

local function lookup_context_artists(artists: { LocalArtist }?): { LookupArtist }?
	if artists == nil then
		return nil
	end
	local lookup_artists: { LookupArtist } = {}
	for _, artist: LocalArtist in artists do
		local lookup_artist: LookupArtist = {
			artist_name = artist.artist_name,
		}
		table.insert(lookup_artists, lookup_artist)
	end
	return lookup_artists
end

local function lookup_context_tracks(tracks: { LocalTrack }?): { LookupTrack }?
	if tracks == nil then
		return nil
	end
	local lookup_tracks: { LookupTrack } = {}
	for _, track: LocalTrack in tracks do
		local lookup_track: LookupTrack = {
			track_title = track.track_title,
			track_total = track.track_total,
			artists = lookup_context_artists(track.artists),
			disc = track.disc,
			track = track.track,
			duration_ms = track.duration_ms,
		}
		table.insert(lookup_tracks, lookup_track)
	end
	return lookup_tracks
end

function module.normalize_lookup_context(release_context: ReleaseContext?): LookupContext?
	if release_context == nil then
		return nil
	end
	return {
		lookup_hints = release_context.lookup_hints,
		release_title = release_context.release_title,
		artists = lookup_context_artists(release_context.artists),
		tracks = lookup_context_tracks(release_context.tracks),
		library = release_context.library,
		library_id = release_context.library_id,
	}
end

local function edition_hints_from_context(release_context: ReleaseContext?): EditionHints?
	if not release_context then
		return nil
	end
	local media_formats = release_context.media_formats
	local barcode = release_context.barcode
	if (media_formats == nil or #media_formats == 0) and barcode == nil then
		return nil
	end
	return { media_formats = media_formats, barcode = barcode }
end

function module.from_context(release_context: ReleaseContext): LocalRelease
	local lookup_context = module.normalize_lookup_context(release_context)
	local inferred_artist_hint, inferred_release_hint = infer_lookup_hints(lookup_context)
	local release_title = if lookup_context then lookup_context.release_title else nil
	if (is_blank_text(release_title) or is_placeholder_text(release_title)) and inferred_release_hint then
		release_title = inferred_release_hint
	end

	local artist_name = lookup_context
		and lookup_context.artists
		and lookup_context.artists[1]
		and lookup_context.artists[1].artist_name
	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end
	local local_tracks = release_context.tracks
	local compilation_release = is_compilation(local_tracks)
	if not artist_name then
		if compilation_release then
			artist_name = nil -- signals title-only search
		else
			artist_name = artist_name_from_tracks(local_tracks)
		end
	end
	if is_blank_text(artist_name) or is_generic_artist(artist_name) then
		artist_name = nil
	end
	if not artist_name and inferred_artist_hint and not is_generic_artist(inferred_artist_hint) then
		artist_name = inferred_artist_hint
	end

	local preferred_language: string? = nil
	local preferred_country: string? = nil
	local library = if lookup_context then lookup_context.library else nil
	local library_id = if lookup_context then lookup_context.library_id else nil
	if not library and library_id then
		local lib_results = libraries.list(library_id)
		local first_library = if lib_results then lib_results[1] else nil
		if first_library then
			library = {
				language = first_library.language,
				country = first_library.country,
			}
		end
	end
	if library then
		preferred_language = normalize_preferred_language(library.language)
		preferred_country = normalize_preferred_country(library.country)
	end

	return {
		context = release_context,
		artist_name = artist_name,
		release_title = release_title,
		track_count = release_context_track_count(release_context),
		preferred_language = preferred_language,
		preferred_country = preferred_country,
		compilation_release = compilation_release,
		edition_hints = edition_hints_from_context(release_context),
		artist_ids = nil,
		artist_missing_from_musicbrainz = false,
		verified_recording_ids = nil,
		rejected_release_ids = {},
	}
end

local function derive(local_release: LocalRelease): LocalRelease
	local updated = table.clone(local_release)
	updated.rejected_release_ids = {}
	return updated
end

-- The same local release with a preferred country inferred during search.
function module.with_preferred_country(local_release: LocalRelease, preferred_country: string?): LocalRelease
	local updated = derive(local_release)
	updated.preferred_country = preferred_country
	return updated
end

-- The same local release with the artist search's results.
function module.with_artist_search(
	local_release: LocalRelease,
	artist_ids: ArtistIdSet?,
	artist_missing_from_musicbrainz: boolean
): LocalRelease
	local updated = derive(local_release)
	updated.artist_ids = artist_ids
	updated.artist_missing_from_musicbrainz = artist_missing_from_musicbrainz
	return updated
end

-- The same local release, with `recording_ids` known to be its audio.
function module.with_verified_recordings(
	local_release: LocalRelease,
	recording_ids: { [string]: boolean }
): LocalRelease
	local updated = derive(local_release)
	updated.verified_recording_ids = recording_ids
	return updated
end

function module.for_each_local_artist(release_context: ReleaseContext, visit: (LocalArtist) -> ())
	if release_context.artists then
		for _, artist: LocalArtist in release_context.artists do
			visit(artist)
		end
	end
	for _, track: LocalTrack in release_context.tracks do
		local artists = track.artists
		if artists == nil then
			continue
		end
		for _, artist: LocalArtist in artists do
			visit(artist)
		end
	end
end

return module
