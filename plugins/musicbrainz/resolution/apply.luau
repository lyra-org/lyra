--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local genres = require("@lyra/genres")
local metadata = require("@lyra/metadata")
local serde = require("@harmony/serde")

local local_release = require("./local_release")
local musicbrainz_api = require("./musicbrainz_api")
local text_helpers = require("./text_helpers")
local track_helpers = require("./track_helpers")

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type LocalRelease = local_release.LocalRelease
type LocalArtist = track_helpers.LocalArtist
type LocalTrack = track_helpers.LocalTrack
type ReleaseArtist = track_helpers.ReleaseArtist
type ReleaseArtistCredit = track_helpers.ReleaseArtistCredit
type ReleaseGroup = track_helpers.ReleaseGroup
type ReleaseGenre = track_helpers.ReleaseGenre
type Release = track_helpers.Release
type Relation = musicbrainz_api.Relation
type RecordingResult = musicbrainz_api.RecordingResult
type EnsureArtistRequest = metadata.EnsureArtistRequest
type ArtistType = metadata.ArtistType
type Provider = metadata.Provider

local ArtistType = metadata.ArtistType

local mb_get = musicbrainz_api.mb_get
local json_string_field = musicbrainz_api.json_string_field
local json_array_field = musicbrainz_api.json_array_field
local normalize_release_artist = musicbrainz_api.normalize_release_artist
local normalize_recording_result = musicbrainz_api.normalize_recording_result
local normalize_relations = musicbrainz_api.normalize_relations
local normalize_title = text_helpers.normalize_title
local trim_text = text_helpers.trim_text
local is_generic_artist = text_helpers.is_generic_artist
local compute_track_count = track_helpers.compute_track_count
local assign_release_tracks = track_helpers.assign_release_tracks
local should_preserve_local_track_positions = track_helpers.should_preserve_local_track_positions

local provider: Provider? = nil

local module = {}

function module.set_provider(provider_instance: Provider)
	provider = provider_instance
end

function module.set_track_id(track_db_id: number, id_type: string, id_value: string)
	local current_provider = provider
	if current_provider ~= nil then
		local track_layer = current_provider:layer(track_db_id)
		track_layer:set_id(id_type, id_value)
		track_layer:save()
	end
end

-- Does nothing once a request has failed on this refresh: an outage must not
-- discard a match that a later refresh would confirm.
function module.mark_unmatched(db_id: number, id_types: { string })
	local current_provider = provider
	if current_provider ~= nil and not musicbrainz_api.request_failed() then
		current_provider:mark_unmatched(db_id, id_types)
	end
end

local function artist_type_for_musicbrainz(mb_artist_type: string?): ArtistType?
	if mb_artist_type == nil then
		return nil
	end

	local normalized_type = string.lower(mb_artist_type)
	if normalized_type == "person" then
		return ArtistType.Person
	elseif normalized_type == "group" then
		return ArtistType.Group
	elseif normalized_type == "character" then
		return ArtistType.Character
	elseif normalized_type == "orchestra" then
		return ArtistType.Orchestra
	elseif normalized_type == "choir" then
		return ArtistType.Choir
	end

	return nil
end

local function set_artist_metadata(db_id: number?, mb_artist: ReleaseArtist?): boolean
	local current_provider = provider
	if not db_id or mb_artist == nil or not mb_artist.id or current_provider == nil then
		return false
	end
	local artist_layer = current_provider:layer(db_id)
	artist_layer:set_field("artist_name", mb_artist.name)
	if mb_artist["sort-name"] then
		artist_layer:set_field("sort_name", mb_artist["sort-name"])
	end
	if mb_artist["type"] then
		artist_layer:set_field("artist_type", string.lower(mb_artist["type"]))
	end
	artist_layer:set_id("artist_id", mb_artist.id)
	artist_layer:save()
	return true
end

local function ensure_artist_entity(mb_artist: ReleaseArtist?): number?
	local current_provider = provider
	if mb_artist == nil or not mb_artist.id or current_provider == nil then
		return nil
	end
	local artist_name = trim_text(mb_artist.name)
	if artist_name == "" or is_generic_artist(artist_name) then
		return nil
	end
	local request: EnsureArtistRequest = {
		id_type = "artist_id",
		id_value = mb_artist.id,
		artist_name = artist_name,
	}
	if mb_artist["sort-name"] then
		request.sort_name = mb_artist["sort-name"]
	end
	local artist_type = artist_type_for_musicbrainz(mb_artist["type"])
	if artist_type then
		request.artist_type = artist_type
	end
	local ok, artist_id = pcall(current_provider.ensure_artist, current_provider, request)
	if not ok or not artist_id then
		return nil
	end
	return artist_id
end

local function reconcile_artist_credits(
	owner_id: number,
	credits: { ReleaseArtistCredit },
	tagged_artist_mb_ids: { [number]: string }
)
	local current_provider = provider
	if current_provider == nil or #credits < 2 then
		return
	end
	local artist_ids: { [string]: number } = {}
	for artist_id, mb_id in tagged_artist_mb_ids do
		artist_ids[mb_id] = artist_id
	end
	local resolved: { metadata.ArtistCreditInput } = {}
	for _, credit in credits do
		local mb_id = credit.artist and credit.artist.id
		local artist_id = if mb_id then artist_ids[mb_id] else nil
		if artist_id == nil then
			artist_id = ensure_artist_entity(credit.artist)
		end
		local name = trim_text(credit.name or (credit.artist and credit.artist.name))
		if artist_id == nil or name == "" then
			return
		end
		table.insert(resolved, {
			artist_id = artist_id,
			name = name,
			join_phrase = credit.joinphrase,
		})
	end
	current_provider:reconcile_artist_credits(owner_id, resolved)
end

type ArtistRelationType = metadata.ArtistRelationType
local ArtistRelationType = metadata.ArtistRelationType

local MB_ARTIST_RELATION_MAP: { [string]: ArtistRelationType? } = {
	["voice actor"] = ArtistRelationType.VoiceActor,
	["member of band"] = ArtistRelationType.MemberOf,
}

type PendingArtistRelation = {
	from_artist_db_id: number,
	to_artist_db_id: number,
	relation_type: ArtistRelationType,
	attributes: { [string]: string },
}

local function musicbrainz_artist_type_has_imported_relations(mb_artist_type: string?): boolean
	local normalized_type = if mb_artist_type then string.lower(mb_artist_type) else nil
	return normalized_type == "character" or normalized_type == "group"
end

local function collect_artist_relation_attributes(target: { [string]: string }, attributes: { string }?)
	if attributes == nil then
		return
	end
	for _, raw_attr in attributes do
		local attr = trim_text(raw_attr)
		if attr ~= "" then
			target[attr] = attr
		end
	end
end

local function artist_relation_attributes_string(attributes: { [string]: string }): string?
	local parts: { string } = {}
	for _, attr in attributes do
		table.insert(parts, attr)
	end
	table.sort(parts)
	if #parts == 0 then
		return nil
	end
	return table.concat(parts, ", ")
end

local function import_artist_relations_from_response(current_artist_db_id: number, response: JsonObject): boolean
	local current_provider = provider
	if current_provider == nil then
		return false
	end

	local relations = normalize_relations(json_array_field(response, "relations"))
	local pending_relations: { [string]: PendingArtistRelation } = {}
	for _, relation in relations do
		local target_type = relation["target-type"]
		if target_type ~= "artist" then
			continue
		end

		local relation_type_str = relation["type"]
		if type(relation_type_str) ~= "string" then
			continue
		end
		local relation_type = MB_ARTIST_RELATION_MAP[string.lower(relation_type_str)]
		if relation_type == nil then
			continue
		end

		local direction = relation.direction
		if direction ~= "forward" and direction ~= "backward" then
			continue
		end

		local related_artist = relation.artist
		if related_artist == nil or not related_artist.id or not related_artist.name then
			continue
		end

		local related_artist_db_id = ensure_artist_entity({
			id = related_artist.id,
			name = related_artist.name,
			["sort-name"] = related_artist["sort-name"],
			["type"] = related_artist["type"],
		})
		if related_artist_db_id == nil then
			continue
		end

		local from_artist_db_id = if direction == "forward" then current_artist_db_id else related_artist_db_id
		local to_artist_db_id = if direction == "forward" then related_artist_db_id else current_artist_db_id
		local relation_key = `{from_artist_db_id}:{to_artist_db_id}:{string.lower(relation_type_str)}`
		local pending = pending_relations[relation_key]
		if pending == nil then
			pending = {
				from_artist_db_id = from_artist_db_id,
				to_artist_db_id = to_artist_db_id,
				relation_type = relation_type,
				attributes = {},
			}
			pending_relations[relation_key] = pending
		end
		collect_artist_relation_attributes(pending.attributes, relation.attributes)
	end

	local relation_keys: { string } = {}
	for relation_key in pending_relations do
		table.insert(relation_keys, relation_key)
	end
	table.sort(relation_keys)

	local linked_count = 0
	for _, relation_key in relation_keys do
		local pending = pending_relations[relation_key]
		local attributes_str = artist_relation_attributes_string(pending.attributes)
		local link_ok = pcall(
			current_provider.link_artist_relation,
			current_provider,
			pending.from_artist_db_id,
			pending.to_artist_db_id,
			pending.relation_type,
			attributes_str
		)
		if link_ok then
			linked_count += 1
		end
	end

	return linked_count > 0
end

local function fetch_and_link_artist_relations(
	from_artist_db_id: number,
	mb_artist_id: string,
	mb_artist_type: string?
): boolean
	if not musicbrainz_artist_type_has_imported_relations(mb_artist_type) then
		return false
	end

	local response = mb_get(`/artist/{mb_artist_id}?inc=artist-rels`)
	if response == nil then
		return false
	end

	return import_artist_relations_from_response(from_artist_db_id, response)
end

local function refresh_artist_by_id(artist_db_id: number, mb_artist_id: string): boolean
	local response = mb_get(`/artist/{mb_artist_id}?inc=artist-rels`)
	if response == nil then
		return false
	end

	set_artist_metadata(artist_db_id, normalize_release_artist(response))
	return import_artist_relations_from_response(artist_db_id, response)
end

type Work = { id: string?, relations: { Relation }? }

-- The work a recording performs, preferring a performance relationship.
local function track_work_from_relations(relations: { Relation }?): Work?
	local fallback: Work? = nil
	if relations == nil then
		return nil
	end
	for _, relation: Relation in relations do
		if relation["target-type"] ~= "work" then
			continue
		end
		local work = relation.work
		if work == nil or type(work.id) ~= "string" or work.id == "" then
			continue
		end
		local relation_type = string.lower(relation.type or "")
		if relation_type == "performance" then
			return work
		end
		if not fallback then
			fallback = work
		end
	end
	return fallback
end

type RecordingCache = { [string]: RecordingResult | false }

local function fetch_recording_result(recording_id: string?, recording_cache: RecordingCache?): RecordingResult?
	if type(recording_id) ~= "string" or recording_id == "" then
		return nil
	end
	if recording_cache and recording_cache[recording_id] ~= nil then
		local cached = recording_cache[recording_id]
		if cached == false then
			return nil
		end
		return cached :: RecordingResult
	end
	local recording_response = mb_get(`/recording/{recording_id}?inc=work-rels+work-level-rels+artist-rels`)
	local result = if recording_response then normalize_recording_result(recording_response) else nil
	if recording_cache then
		recording_cache[recording_id] = result or false
	end
	return result
end

type CreditType = metadata.CreditType
local CreditType = metadata.CreditType

local MB_RELATION_TO_CREDIT: { [string]: { type: CreditType, detail: string? }? } = {
	-- performance
	vocal = { type = CreditType.Vocalist },
	instrument = { type = CreditType.Instrumentalist },
	programming = { type = CreditType.Instrumentalist, detail = "programming" },
	conductor = { type = CreditType.Conductor },
	-- composition
	arranger = { type = CreditType.Arranger },
	composer = { type = CreditType.Composer },
	lyricist = { type = CreditType.Lyricist },
	writer = { type = CreditType.Writer },
	-- production
	producer = { type = CreditType.Producer },
	engineer = { type = CreditType.Engineer },
	["audio engineer"] = { type = CreditType.Engineer, detail = "audio" },
	["sound engineer"] = { type = CreditType.Engineer, detail = "sound" },
	mix = { type = CreditType.Mixer },
	recording = { type = CreditType.Engineer, detail = "recording" },
	["recording engineer"] = { type = CreditType.Engineer, detail = "recording" },
	["balance engineer"] = { type = CreditType.Engineer, detail = "balance" },
	["instrument technician"] = { type = CreditType.Engineer, detail = "instrument technician" },
	editor = { type = CreditType.Engineer, detail = "editor" },
	["instrument arranger"] = { type = CreditType.Arranger, detail = "instrument" },
	-- remix
	remixer = { type = CreditType.Remixer },
	["dj-mixer"] = { type = CreditType.Mixer, detail = "DJ mix" },
}

local function link_artist_credits_from_relations(track_db_id: number, relations: { Relation })
	local current_provider = provider
	if current_provider == nil then
		return
	end
	for _, relation: Relation in relations do
		if relation["target-type"] ~= "artist" then
			continue
		end
		local relation_type = string.lower(relation.type or "")
		local mapping = MB_RELATION_TO_CREDIT[relation_type]
		if mapping == nil then
			continue
		end
		local artist = relation.artist
		if artist == nil or not artist.id or not artist.name then
			continue
		end

		local artist_id = ensure_artist_entity({
			id = artist.id,
			name = artist.name,
			["sort-name"] = artist["sort-name"],
		} :: ReleaseArtist)
		if not artist_id then
			continue
		end

		local detail = mapping.detail
		local attributes = relation.attributes
		if attributes and #attributes > 0 then
			detail = table.concat(attributes, ", ")
		end

		pcall(current_provider.link_credit, current_provider, track_db_id, artist_id, mapping.type, detail)
	end
end

local function joinphrase_opens_character_voice(joinphrase: string?): boolean
	if joinphrase == nil then
		return false
	end
	local trimmed = string.gsub(joinphrase, "^%s+", "")
	return string.find(trimmed, "^%([cC][vV][:%.%s]") ~= nil or string.find(trimmed, "^（[cC][vV]：") ~= nil
end

local function joinphrase_closes_credit_group(joinphrase: string?): boolean
	if joinphrase == nil then
		return false
	end
	return string.find(joinphrase, ")", 1, true) ~= nil or string.find(joinphrase, "）", 1, true) ~= nil
end

local function link_character_voice_credits_from_track_artist_credits(
	track_db_id: number,
	credits: { ReleaseArtistCredit }
): { [string]: boolean }
	local current_provider = provider
	local linked_mb_artist_ids: { [string]: boolean } = {}
	if current_provider == nil then
		return linked_mb_artist_ids
	end

	local in_character_voice_credit = false
	for _, credit: ReleaseArtistCredit in credits do
		local artist = credit.artist
		if in_character_voice_credit and artist and artist.id and artist.name then
			local artist_id = ensure_artist_entity(artist)
			if artist_id then
				pcall(
					current_provider.link_credit,
					current_provider,
					track_db_id,
					artist_id,
					CreditType.Vocalist,
					"character voice"
				)
				linked_mb_artist_ids[artist.id] = true
			end
		end

		local joinphrase = credit.joinphrase
		if artist and artist["type"] == "Character" and joinphrase_opens_character_voice(joinphrase) then
			in_character_voice_credit = true
		elseif in_character_voice_credit and joinphrase_closes_credit_group(joinphrase) then
			in_character_voice_credit = false
		end
	end

	return linked_mb_artist_ids
end

local function build_credit_lookup(credits: { ReleaseArtistCredit }): { [string]: ReleaseArtist }
	local by_name: { [string]: ReleaseArtist } = {}
	for _, credit: ReleaseArtistCredit in credits do
		local mb_artist = credit.artist
		if mb_artist and credit.name then
			by_name[string.lower(credit.name)] = mb_artist
		end
	end
	return by_name
end

local function match_credit(
	local_name_lower: string,
	credits: { ReleaseArtistCredit },
	by_name: { [string]: ReleaseArtist }
): ReleaseArtist?
	local exact = by_name[local_name_lower]
	if exact then
		return exact
	end
	for _, credit: ReleaseArtistCredit in credits do
		if not credit.artist or not credit.name then
			continue
		end
		local jp = credit.joinphrase or ""
		if not string.find(jp, "(", 1, true) and not string.find(jp, "（", 1, true) then
			continue
		end
		local prefix = string.lower(credit.name)
		if #local_name_lower > #prefix and string.sub(local_name_lower, 1, #prefix) == prefix then
			local rest = string.gsub(string.sub(local_name_lower, #prefix + 1), "^%s+", "")
			if string.sub(rest, 1, 1) == "(" or string.sub(rest, 1, 3) == "（" then
				return credit.artist
			end
		end
	end
	return nil
end

local function process_release(
	release: Release,
	local_release_info: LocalRelease,
	release_group_id: string?,
	release_group_info: ReleaseGroup?
): boolean
	local release_context = local_release_info.context
	local current_provider = provider
	if current_provider == nil then
		return false
	end
	local release_db_id = release_context.db_id
	local release_id = release.id
	if release_db_id == nil or release_id == nil then
		return false
	end

	local rg_info = release_group_info or release["release-group"]
	local release_layer = current_provider:layer(release_db_id)
	local release_title = release.title
	local release_track_count = compute_track_count(release)

	if #release_context.tracks == 1 and release_track_count > 1 then
		local local_release_title = release_context.release_title
		local local_track_title = release_context.tracks[1] and release_context.tracks[1].track_title
		local norm_local_release = normalize_title(local_release_title)
		local norm_local_track = normalize_title(local_track_title)
		if
			norm_local_release ~= ""
			and norm_local_release == norm_local_track
			and normalize_title(release.title) ~= norm_local_release
		then
			release_title = local_release_title
		end
	end

	release_layer:set_field("release_title", release_title)

	local date_str = release.date or (rg_info and rg_info["first-release-date"])
	local rg_url_data: JsonObject? = nil
	if not date_str then
		local rg_id_for_date = release_group_id or (rg_info and rg_info.id)
		if rg_id_for_date then
			-- Include url-rels to extract WikiData QID in the same request.
			rg_url_data = mb_get(`/release-group/{rg_id_for_date}?inc=url-rels`)
			if rg_url_data then
				date_str = json_string_field(rg_url_data, "first-release-date") or date_str
			end
		end
	end
	if date_str then
		release_layer:set_field("release_date", date_str)
	end

	local genre_names: { JsonValue } = {}
	local release_genres = release.genres
	if release_genres ~= nil then
		for _, genre: ReleaseGenre in release_genres do
			if genre.name then
				table.insert(genre_names, genre.name)
				genres.resolve({
					name = genre.name,
					external_id = if genre.id
						then {
							provider_id = "musicbrainz",
							id_type = "genre_id",
							id_value = genre.id,
						}
						else nil,
				})
			end
		end
	end
	if #genre_names > 0 then
		release_layer:set_field("genres", genre_names)
	end

	local primary_type: string? = if rg_info then rg_info["primary-type"] else nil
	if not primary_type and rg_url_data then
		primary_type = json_string_field(rg_url_data, "primary-type")
	end
	if primary_type then
		release_layer:set_field("release_type", string.lower(primary_type))
	end

	release_layer:set_id("release_id", release_id)
	local rg_id = release_group_id or (rg_info and rg_info.id)
	if rg_id then
		release_layer:set_id("release_group_id", rg_id)

		-- Cooperation: if we already fetched release-group data (date fallback),
		-- extract WikiData QID from its URL relationships at zero extra cost.
		-- No dedicated fetch — the WikiData plugin handles releases MB doesn't cover.
		if rg_url_data then
			for _, relation in normalize_relations(json_array_field(rg_url_data, "relations")) do
				local resource = relation.url and relation.url.resource
				if relation.type == "wikidata" and resource then
					local qid = string.match(resource, "(Q%d+)$")
					if qid then
						release_layer:set_id("wikidata_qid", qid)
						break
					end
				end
			end
		end
	end
	release_layer:save()

	local artist_credits = release["artist-credit"]
	local tagged_artist_ids: { [number]: boolean } = {}
	local tagged_artist_mb_ids: { [number]: string } = {}
	local fetched_relations_for: { [string]: boolean } = {}

	local function maybe_fetch_artist_relations(db_id: number, mb_artist: ReleaseArtist)
		if mb_artist.id == nil or fetched_relations_for[mb_artist.id] then
			return
		end
		fetched_relations_for[mb_artist.id] = true
		fetch_and_link_artist_relations(db_id, mb_artist.id, mb_artist["type"])
	end

	local function tag_local_artist(db_id: number?, mb_artist: ReleaseArtist?): boolean
		if not db_id or mb_artist == nil or mb_artist.id == nil then
			return false
		end
		ensure_artist_entity(mb_artist)
		if set_artist_metadata(db_id, mb_artist) then
			tagged_artist_ids[db_id] = true
			tagged_artist_mb_ids[db_id] = mb_artist.id
			maybe_fetch_artist_relations(db_id, mb_artist)
			return true
		end
		return false
	end

	if artist_credits and release_context.artists then
		local matched_release_mb_artist_ids: { [string]: boolean } = {}
		local mb_release_artists_by_name = build_credit_lookup(artist_credits)

		for _, local_artist: LocalArtist in release_context.artists do
			if local_artist.db_id and not tagged_artist_ids[local_artist.db_id] then
				local local_name = string.lower(local_artist.artist_name or "")
				local matched = match_credit(local_name, artist_credits, mb_release_artists_by_name)
				if matched and matched.id and tag_local_artist(local_artist.db_id, matched) then
					matched_release_mb_artist_ids[matched.id] = true
				end
			end
		end

		local untagged_ids: { [number]: boolean } = {}
		local untagged_first: LocalArtist? = nil
		local unique_untagged = 0
		for _, local_artist: LocalArtist in release_context.artists do
			local local_artist_id = local_artist.db_id
			if local_artist_id and not tagged_artist_ids[local_artist_id] and not untagged_ids[local_artist_id] then
				untagged_ids[local_artist_id] = true
				unique_untagged += 1
				if untagged_first == nil then
					untagged_first = local_artist
				end
			end
		end

		local first_mb_credit = artist_credits[1]
		local first_mb_artist = if first_mb_credit then first_mb_credit.artist else nil
		if
			unique_untagged == 1
			and #artist_credits == 1
			and untagged_first ~= nil
			and first_mb_artist ~= nil
			and first_mb_artist.id ~= nil
			and not matched_release_mb_artist_ids[first_mb_artist.id]
			and tag_local_artist(untagged_first.db_id, first_mb_artist)
		then
			matched_release_mb_artist_ids[first_mb_artist.id] = true
		end

		for _, mb_credit: ReleaseArtistCredit in artist_credits do
			local mb_artist = mb_credit.artist
			if mb_artist and mb_artist.id and not matched_release_mb_artist_ids[mb_artist.id] then
				local ensured_artist_id = ensure_artist_entity(mb_artist)
				if ensured_artist_id then
					current_provider:link_credit(release_db_id, ensured_artist_id, nil, nil)
					tagged_artist_ids[ensured_artist_id] = true
					tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
					matched_release_mb_artist_ids[mb_artist.id] = true
					maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
				end
			end
		end
	end

	if artist_credits then
		reconcile_artist_credits(release_db_id, artist_credits, tagged_artist_mb_ids)
	end

	if #release_context.tracks > 0 then
		local local_tracks, assignments = assign_release_tracks(release_context.tracks, release)
		local release_media = release.media
		local release_disc_total = if release_media ~= nil then #release_media else 0
		local inferred_disc_total = 0
		for i: number, local_track: LocalTrack in local_tracks do
			local mb_track = assignments[i]
			if mb_track and local_track.db_id then
				local assigned_disc = mb_track.disc
				if assigned_disc and assigned_disc > inferred_disc_total then
					inferred_disc_total = assigned_disc
				end
			end
		end

		local resolved_disc_total = if release_disc_total > 0 then release_disc_total else inferred_disc_total
		local first_local_track = local_tracks[1]
		local first_track_total = if first_local_track ~= nil then first_local_track.track_total else nil
		local preserve_single_track_position = #local_tracks == 1
			and first_track_total ~= nil
			and first_track_total <= 1
			and first_local_track ~= nil
			and first_local_track.track ~= nil
		local preserve_local_multidisc_position = should_preserve_local_track_positions(local_tracks, release)
		local preserve_local_position = preserve_single_track_position or preserve_local_multidisc_position
		local should_lookup_track_work = #local_tracks == 1
		local recording_cache: RecordingCache = {}
		local track_artist_count = 0

		for i: number, local_track: LocalTrack in local_tracks do
			local mb_track = assignments[i]
			if mb_track and local_track.db_id then
				local track_layer = current_provider:layer(local_track.db_id)
				track_layer:set_field("track_title", mb_track.title)
				if preserve_local_position then
					if local_track.disc ~= nil then
						track_layer:set_field("disc", local_track.disc)
					else
						track_layer:set_field("disc", mb_track.disc)
					end
					if local_track.track ~= nil then
						track_layer:set_field("track", local_track.track)
					else
						track_layer:set_field("track", mb_track.track)
					end
				else
					track_layer:set_field("disc", mb_track.disc)
					track_layer:set_field("track", mb_track.track)
				end
				if resolved_disc_total > 0 then
					track_layer:set_field("disc_total", resolved_disc_total)
				end
				if mb_track.release_track_id then
					track_layer:set_id("release_track_id", mb_track.release_track_id)
				end
				local recording_relations: { Relation }? = nil
				if mb_track.recording_id then
					track_layer:set_id("recording_id", mb_track.recording_id)
					local inline_relations = mb_track.recording_relations
					if inline_relations then
						recording_relations = inline_relations :: { Relation }?
					end
					local work = track_work_from_relations(recording_relations)
					if should_lookup_track_work and work == nil and recording_relations == nil then
						local result = fetch_recording_result(mb_track.recording_id, recording_cache)
						if result then
							work = track_work_from_relations(result.relations)
							recording_relations = result.relations
						end
					end
					if work and work.id then
						track_layer:set_field("work_id", work.id)
						if work.relations and local_track.db_id then
							link_artist_credits_from_relations(local_track.db_id, work.relations)
						end
					end
				end
				track_layer:save()

				if recording_relations and local_track.db_id then
					link_artist_credits_from_relations(local_track.db_id, recording_relations)
				end

				if mb_track.artist_credits and local_track.artists then
					local mb_credit_list = mb_track.artist_credits
					local mb_artist_by_name = build_credit_lookup(mb_credit_list)
					local matched_mb_artist_ids: { [string]: boolean } = {}
					local character_voice_artist_ids =
						link_character_voice_credits_from_track_artist_credits(local_track.db_id, mb_credit_list)
					for mb_artist_id in character_voice_artist_ids do
						matched_mb_artist_ids[mb_artist_id] = true
					end

					for _, track_artist: LocalArtist in local_track.artists do
						local track_artist_id = track_artist.db_id
						if track_artist_id ~= nil then
							local previously_tagged_mb_id = tagged_artist_mb_ids[track_artist_id]
							if previously_tagged_mb_id then
								matched_mb_artist_ids[previously_tagged_mb_id] = true
							elseif not tagged_artist_ids[track_artist_id] then
								local local_name = string.lower(track_artist.artist_name or "")
								local matched_mb_artist = match_credit(local_name, mb_credit_list, mb_artist_by_name)
								if matched_mb_artist and matched_mb_artist.id then
									matched_mb_artist_ids[matched_mb_artist.id] = true
									if tag_local_artist(track_artist_id, matched_mb_artist) then
										track_artist_count += 1
									end
								end
							end
						end
					end

					local unmatched_mb_artists: { ReleaseArtist } = {}
					for _, mb_credit: ReleaseArtistCredit in mb_credit_list do
						local mb_artist = mb_credit.artist
						if mb_artist and mb_artist.id and not matched_mb_artist_ids[mb_artist.id] then
							table.insert(unmatched_mb_artists, mb_artist)
						end
					end

					for _, mb_artist: ReleaseArtist in unmatched_mb_artists do
						if local_track.db_id and mb_artist.id and not matched_mb_artist_ids[mb_artist.id] then
							local ensured_artist_id = ensure_artist_entity(mb_artist)
							if ensured_artist_id then
								current_provider:link_credit(local_track.db_id, ensured_artist_id, nil, nil)
								tagged_artist_ids[ensured_artist_id] = true
								tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
								matched_mb_artist_ids[mb_artist.id] = true
								track_artist_count += 1
								maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
							end
						end
					end
					if local_track.db_id and next(character_voice_artist_ids) == nil then
						reconcile_artist_credits(local_track.db_id, mb_credit_list, tagged_artist_mb_ids)
					end
				end
			end
		end

		local release_primary = artist_credits and artist_credits[1] and artist_credits[1].artist
		local should_override_with_track_artist = #local_tracks == 1
			and release_track_count > 1
			and release_primary ~= nil
			and is_generic_artist(release_primary.name)
		if should_override_with_track_artist then
			local assigned = assignments[1]
			local mb_credit_list = assigned and assigned.artist_credits
			local mb_track_artist = mb_credit_list and mb_credit_list[1] and mb_credit_list[1].artist
			if mb_track_artist and mb_track_artist.id then
				local updated_ids: { [number]: boolean } = {}
				local function apply_track_artist(entity: LocalArtist)
					if entity.db_id == nil or updated_ids[entity.db_id] then
						return
					end
					updated_ids[entity.db_id] = true
					if tag_local_artist(entity.db_id, mb_track_artist) then
						track_artist_count += 1
					end
				end

				if release_context.artists then
					for _, artist: LocalArtist in release_context.artists do
						apply_track_artist(artist)
					end
				end
				local first_track_artists = local_tracks[1].artists
				if first_track_artists then
					for _, artist: LocalArtist in first_track_artists do
						apply_track_artist(artist)
					end
				end
			end
		end
	end

	return true
end

module.refresh_artist_by_id = refresh_artist_by_id
module.process_release = process_release

return module
