--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local artists = require("@lyra/artists")
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
type ArtistReader = (ids: { string }) -> { [string]: artists.Artist }

type ArtistRelationType = metadata.ArtistRelationType
type LayerRelationInput = metadata.LayerRelationInput
-- One relation type's whole set of artists relating to a layer's artist.
type RelationSet = { relation_type: ArtistRelationType, relations: { LayerRelationInput } }

local ArtistType = metadata.ArtistType
local ArtistRelationType = metadata.ArtistRelationType

local enrichment_get = musicbrainz_api.enrichment_get
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
local created_before = track_helpers.created_before

local provider: Provider? = nil
local read_artists: ArtistReader = artists.get

local module = {}

function module.set_provider(provider_instance: Provider)
	provider = provider_instance
end

function module.set_artist_reader(reader: ArtistReader)
	read_artists = reader
end

-- Fills in the creation times `created_at_ms` lacks for `artist_ids`, read in
-- one batch.
local function read_artist_creation_times(created_at_ms: { [string]: number }, artist_ids: { string })
	local missing: { string } = {}
	for _, artist_id in artist_ids do
		if created_at_ms[artist_id] == nil and not table.find(missing, artist_id) then
			table.insert(missing, artist_id)
		end
	end
	if #missing == 0 then
		return
	end
	for artist_id, artist in read_artists(missing) do
		if artist.created_at_ms ~= nil then
			created_at_ms[artist_id] = artist.created_at_ms
		end
	end
end

function module.set_track_id(track_id: string, id_type: string, id_value: string)
	local current_provider = provider
	if current_provider ~= nil then
		local track_layer = current_provider:layer(track_id)
		track_layer:set_id(id_type, id_value)
		track_layer:save()
	end
end

-- Does nothing once a request has failed on this refresh: an outage must not
-- discard a match that a later refresh would confirm.
function module.mark_unmatched(node_id: string, id_types: { string })
	local current_provider = provider
	if current_provider ~= nil and not musicbrainz_api.request_failed() then
		current_provider:mark_unmatched(node_id, id_types)
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

local function set_artist_metadata(
	artist_id: string?,
	mb_artist: ReleaseArtist?,
	relation_sets: { RelationSet }?
): boolean
	local current_provider = provider
	if not artist_id or mb_artist == nil or not mb_artist.id or current_provider == nil then
		return false
	end
	local artist_layer = current_provider:layer(artist_id)
	artist_layer:set_field("artist_name", mb_artist.name)
	if mb_artist["sort-name"] then
		artist_layer:set_field("sort_name", mb_artist["sort-name"])
	end
	local mb_artist_type = mb_artist["type"]
	if mb_artist_type and artist_type_for_musicbrainz(mb_artist_type) then
		artist_layer:set_field("artist_type", string.lower(mb_artist_type))
	end
	artist_layer:set_id("artist_id", mb_artist.id)
	for _, relation_set in relation_sets or {} do
		artist_layer:set_relations(relation_set.relation_type, relation_set.relations)
	end
	artist_layer:save()
	return true
end

local function ensure_artist_entity(mb_artist: ReleaseArtist?): string?
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

local MB_ARTIST_RELATION_MAP: { [string]: ArtistRelationType? } = {
	["voice actor"] = ArtistRelationType.VoiceActor,
	["member of band"] = ArtistRelationType.MemberOf,
}
local IMPORTED_RELATION_TYPES: { string } = { "voice actor", "member of band" }

type PendingArtistRelation = {
	artist_id: string,
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

-- The relations into the artist a response describes, one whole set per
-- imported type. A forward relation belongs to the related artist's own set,
-- which this response does not describe in full, so only backward ones count.
-- One that resolves to the artist itself is left out, since an artist cannot
-- relate to itself.
local function artist_relation_sets(artist_id: string, response: JsonObject): { RelationSet }
	local pending_by_type: { [string]: { [string]: PendingArtistRelation } } = {}
	for _, relation in normalize_relations(json_array_field(response, "relations")) do
		if relation["target-type"] ~= "artist" or relation.direction ~= "backward" then
			continue
		end
		local relation_type_str = relation["type"]
		if type(relation_type_str) ~= "string" then
			continue
		end
		local mb_type = string.lower(relation_type_str)
		if MB_ARTIST_RELATION_MAP[mb_type] == nil then
			continue
		end

		local related_artist = relation.artist
		if related_artist == nil or not related_artist.id or not related_artist.name then
			continue
		end

		local related_artist_id = ensure_artist_entity({
			id = related_artist.id,
			name = related_artist.name,
			["sort-name"] = related_artist["sort-name"],
			["type"] = related_artist["type"],
		})
		if related_artist_id == nil or related_artist_id == artist_id then
			continue
		end

		local pending_by_artist = pending_by_type[mb_type]
		if pending_by_artist == nil then
			pending_by_artist = {}
			pending_by_type[mb_type] = pending_by_artist
		end
		local pending = pending_by_artist[related_artist_id]
		if pending == nil then
			pending = { artist_id = related_artist_id, attributes = {} }
			pending_by_artist[related_artist_id] = pending
		end
		collect_artist_relation_attributes(pending.attributes, relation.attributes)
	end

	local related_artist_ids: { string } = {}
	for _, pending_by_artist in pending_by_type do
		for related_artist_id in pending_by_artist do
			table.insert(related_artist_ids, related_artist_id)
		end
	end
	local created_at_ms: { [string]: number } = {}
	read_artist_creation_times(created_at_ms, related_artist_ids)

	local relation_sets: { RelationSet } = {}
	for _, mb_type in IMPORTED_RELATION_TYPES do
		local relation_type = MB_ARTIST_RELATION_MAP[mb_type]
		if relation_type == nil then
			continue
		end
		local relations: { LayerRelationInput } = {}
		local pending_by_artist: { [string]: PendingArtistRelation } = pending_by_type[mb_type] or {}
		for _, pending in pending_by_artist do
			local relation: LayerRelationInput = {
				artist_id = pending.artist_id,
				attributes = artist_relation_attributes_string(pending.attributes),
			}
			table.insert(relations, relation)
		end
		table.sort(relations, function(a: LayerRelationInput, b: LayerRelationInput): boolean
			return created_before(created_at_ms[a.artist_id], a.artist_id, created_at_ms[b.artist_id], b.artist_id)
		end)
		table.insert(relation_sets, { relation_type = relation_type, relations = relations })
	end
	return relation_sets
end

local function import_artist_relations_from_response(artist_id: string, response: JsonObject): boolean
	return set_artist_metadata(artist_id, normalize_release_artist(response), artist_relation_sets(artist_id, response))
end

local function fetch_and_set_artist_relations(
	from_artist_id: string,
	mb_artist_id: string,
	mb_artist_type: string?
): boolean
	if not musicbrainz_artist_type_has_imported_relations(mb_artist_type) then
		return false
	end

	local response = enrichment_get(`/artist/{mb_artist_id}?inc=artist-rels`)
	if response == nil then
		return false
	end

	return import_artist_relations_from_response(from_artist_id, response)
end

local function refresh_artist_by_id(artist_id: string, mb_artist_id: string): boolean
	local response = mb_get(`/artist/{mb_artist_id}?inc=artist-rels`)
	if response == nil then
		return false
	end

	return import_artist_relations_from_response(artist_id, response)
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
	local recording_response = enrichment_get(`/recording/{recording_id}?inc=work-rels+work-level-rels+artist-rels`)
	local result = if recording_response then normalize_recording_result(recording_response) else nil
	if recording_cache then
		recording_cache[recording_id] = result or false
	end
	return result
end

type CreditType = metadata.CreditType
local CreditType = metadata.CreditType

local MB_RELATION_TO_CREDIT: { [string]: { role: string, detail: string? }? } = {
	-- performance
	vocal = { role = "vocalist" },
	instrument = { role = "instrumentalist" },
	programming = { role = "instrumentalist", detail = "programming" },
	conductor = { role = "conductor" },
	-- composition
	arranger = { role = "arranger" },
	composer = { role = "composer" },
	lyricist = { role = "lyricist" },
	writer = { role = "writer" },
	-- production
	producer = { role = "producer" },
	engineer = { role = "engineer" },
	["audio engineer"] = { role = "engineer", detail = "audio" },
	["sound engineer"] = { role = "engineer", detail = "sound" },
	mix = { role = "mixer" },
	recording = { role = "engineer", detail = "recording" },
	["recording engineer"] = { role = "engineer", detail = "recording" },
	["balance engineer"] = { role = "engineer", detail = "balance" },
	["instrument technician"] = { role = "engineer", detail = "instrument technician" },
	editor = { role = "engineer", detail = "editor" },
	["instrument arranger"] = { role = "arranger", detail = "instrument" },
	-- remix
	remixer = { role = "remixer" },
	["dj-mixer"] = { role = "mixer", detail = "DJ mix" },
}

-- Every credit role, by the name credit lists are gathered under.
local CREDIT_ROLES: { { name: string, role: CreditType } } = {
	{ name = "artist", role = CreditType.Artist },
	{ name = "vocalist", role = CreditType.Vocalist },
	{ name = "instrumentalist", role = CreditType.Instrumentalist },
	{ name = "composer", role = CreditType.Composer },
	{ name = "lyricist", role = CreditType.Lyricist },
	{ name = "arranger", role = CreditType.Arranger },
	{ name = "writer", role = CreditType.Writer },
	{ name = "producer", role = CreditType.Producer },
	{ name = "conductor", role = CreditType.Conductor },
	{ name = "engineer", role = CreditType.Engineer },
	{ name = "mixer", role = CreditType.Mixer },
	{ name = "remixer", role = CreditType.Remixer },
}

type LayerCreditInput = metadata.LayerCreditInput
-- Credits gathered for one release or track, by role name.
type CreditSets = { [string]: { LayerCreditInput } }

local function add_credit(sets: CreditSets, role_name: string, artist_id: string, detail: string?)
	local credits = sets[role_name]
	if credits == nil then
		credits = {}
		sets[role_name] = credits
	end
	for _, existing in credits do
		if existing.artist_id == artist_id and existing.detail == detail then
			return
		end
	end
	local credit: LayerCreditInput = { artist_id = artist_id, detail = detail }
	table.insert(credits, credit)
end

-- Sets each role named in `roles` on the layer; one without credits clears.
local function set_credit_roles(layer: metadata.Layer, sets: CreditSets, roles: { [string]: boolean })
	for _, entry in CREDIT_ROLES do
		if roles[entry.name] then
			layer:set_credits(entry.role, sets[entry.name] or {})
		end
	end
end

-- The artist credits in the provider's order, each artist resolved to its
-- local entity: the one tagged with that MusicBrainz artist, or one ensured.
-- Of several tagged with the same MusicBrainz artist, the one created first
-- gets the credit. `created_at_ms` holds the creation times known so far, and
-- gains any read to settle such a tie.
local function artist_role_credits(
	mb_credits: { ReleaseArtistCredit },
	tagged_artist_mb_ids: { [string]: string },
	skipped_mb_ids: { [string]: boolean },
	created_at_ms: { [string]: number }
): { string }
	local candidates: { [string]: { string } } = {}
	local tied_artist_ids: { string } = {}
	for artist_id, mb_id in tagged_artist_mb_ids do
		local tagged = candidates[mb_id]
		if tagged == nil then
			candidates[mb_id] = { artist_id }
			continue
		end
		if #tagged == 1 then
			table.insert(tied_artist_ids, tagged[1])
		end
		table.insert(tagged, artist_id)
		table.insert(tied_artist_ids, artist_id)
	end
	read_artist_creation_times(created_at_ms, tied_artist_ids)
	local local_ids: { [string]: string } = {}
	for mb_id, tagged in candidates do
		local chosen = tagged[1]
		for index = 2, #tagged do
			local artist_id = tagged[index]
			if created_before(created_at_ms[artist_id], artist_id, created_at_ms[chosen], chosen) then
				chosen = artist_id
			end
		end
		local_ids[mb_id] = chosen
	end
	local artist_ids: { string } = {}
	for _, credit in mb_credits do
		local mb_artist = credit.artist
		local mb_id = mb_artist and mb_artist.id
		if mb_id == nil or skipped_mb_ids[mb_id] then
			continue
		end
		local artist_id = local_ids[mb_id] or ensure_artist_entity(mb_artist)
		if artist_id ~= nil and not table.find(artist_ids, artist_id) then
			table.insert(artist_ids, artist_id)
		end
	end
	return artist_ids
end

local function collect_relation_credits(sets: CreditSets, relations: { Relation })
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

		add_credit(sets, mapping.role, artist_id, detail)
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

local function collect_character_voice_credits(
	sets: CreditSets,
	credits: { ReleaseArtistCredit }
): { [string]: boolean }
	local linked_mb_artist_ids: { [string]: boolean } = {}

	local in_character_voice_credit = false
	for _, credit: ReleaseArtistCredit in credits do
		local artist = credit.artist
		if in_character_voice_credit and artist and artist.id and artist.name then
			local artist_id = ensure_artist_entity(artist)
			if artist_id then
				add_credit(sets, "vocalist", artist_id, "character voice")
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
	-- After a failed request the candidate may stand in for one that went
	-- unchecked, so nothing is applied.
	if current_provider == nil or musicbrainz_api.request_failed() then
		return false
	end
	local release_entity_id = release_context.id
	local release_id = release.id
	if release_entity_id == nil or release_id == nil then
		return false
	end

	local rg_info = release_group_info or release["release-group"]
	local release_layer = current_provider:layer(release_entity_id)
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
			rg_url_data = enrichment_get(`/release-group/{rg_id_for_date}?inc=url-rels`)
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
	local artist_credits = release["artist-credit"]
	local artist_created_at_ms: { [string]: number } = {}
	local_release.for_each_local_artist(release_context, function(artist: LocalArtist)
		if artist.id ~= nil and artist.created_at_ms ~= nil then
			artist_created_at_ms[artist.id] = artist.created_at_ms
		end
	end)
	local tagged_artist_ids: { [string]: boolean } = {}
	local tagged_artist_mb_ids: { [string]: string } = {}
	local fetched_relations_for: { [string]: boolean } = {}

	local function maybe_fetch_artist_relations(artist_id: string, mb_artist: ReleaseArtist)
		if mb_artist.id == nil or fetched_relations_for[mb_artist.id] then
			return
		end
		fetched_relations_for[mb_artist.id] = true
		fetch_and_set_artist_relations(artist_id, mb_artist.id, mb_artist["type"])
	end

	local function tag_local_artist(artist_id: string?, mb_artist: ReleaseArtist?): boolean
		if not artist_id or mb_artist == nil or mb_artist.id == nil then
			return false
		end
		ensure_artist_entity(mb_artist)
		if set_artist_metadata(artist_id, mb_artist) then
			tagged_artist_ids[artist_id] = true
			tagged_artist_mb_ids[artist_id] = mb_artist.id
			maybe_fetch_artist_relations(artist_id, mb_artist)
			return true
		end
		return false
	end

	if artist_credits and release_context.artists then
		local matched_release_mb_artist_ids: { [string]: boolean } = {}
		local mb_release_artists_by_name = build_credit_lookup(artist_credits)

		for _, local_artist: LocalArtist in release_context.artists do
			if local_artist.id and not tagged_artist_ids[local_artist.id] then
				local local_name = string.lower(local_artist.artist_name or "")
				local matched = match_credit(local_name, artist_credits, mb_release_artists_by_name)
				if matched and matched.id and tag_local_artist(local_artist.id, matched) then
					matched_release_mb_artist_ids[matched.id] = true
				end
			end
		end

		local untagged_ids: { [string]: boolean } = {}
		local untagged_first: LocalArtist? = nil
		local unique_untagged = 0
		for _, local_artist: LocalArtist in release_context.artists do
			local local_artist_id = local_artist.id
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
			and tag_local_artist(untagged_first.id, first_mb_artist)
		then
			matched_release_mb_artist_ids[first_mb_artist.id] = true
		end

		for _, mb_credit: ReleaseArtistCredit in artist_credits do
			local mb_artist = mb_credit.artist
			if mb_artist and mb_artist.id and not matched_release_mb_artist_ids[mb_artist.id] then
				local ensured_artist_id = ensure_artist_entity(mb_artist)
				if ensured_artist_id then
					tagged_artist_ids[ensured_artist_id] = true
					tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
					matched_release_mb_artist_ids[mb_artist.id] = true
					maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
				end
			end
		end
	end

	if artist_credits then
		local release_credits: CreditSets = {}
		for _, artist_id in artist_role_credits(artist_credits, tagged_artist_mb_ids, {}, artist_created_at_ms) do
			add_credit(release_credits, "artist", artist_id, nil)
		end
		-- An unresolved list keeps what the release stored rather than clearing it.
		set_credit_roles(release_layer, release_credits, { artist = release_credits.artist ~= nil })
	end
	release_layer:save()

	if #release_context.tracks > 0 then
		local local_tracks, assignments = assign_release_tracks(release_context.tracks, release)
		local release_media = release.media
		local release_disc_total = if release_media ~= nil then #release_media else 0
		local inferred_disc_total = 0
		for i: number, local_track: LocalTrack in local_tracks do
			local mb_track = assignments[i]
			if mb_track and local_track.id then
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
			if mb_track and local_track.id then
				local track_layer = current_provider:layer(local_track.id)
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
				local track_credits: CreditSets = {}
				local credit_roles: { [string]: boolean } = {}
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
						track_layer:set_id("work_id", work.id)
						if work.relations then
							collect_relation_credits(track_credits, work.relations)
						end
					elseif recording_relations ~= nil then
						track_layer:set_id("work_id", "")
					end
				end
				if recording_relations then
					collect_relation_credits(track_credits, recording_relations)
					-- Fetched relations are the whole picture for the roles they map to.
					for _, mapping in MB_RELATION_TO_CREDIT do
						if mapping then
							credit_roles[mapping.role] = true
						end
					end
				end

				if mb_track.artist_credits and local_track.artists then
					local mb_credit_list = mb_track.artist_credits
					local mb_artist_by_name = build_credit_lookup(mb_credit_list)
					local matched_mb_artist_ids: { [string]: boolean } = {}
					local character_voice_artist_ids = collect_character_voice_credits(track_credits, mb_credit_list)
					if next(character_voice_artist_ids) ~= nil then
						credit_roles.vocalist = true
					end
					for mb_artist_id in character_voice_artist_ids do
						matched_mb_artist_ids[mb_artist_id] = true
					end

					for _, track_artist: LocalArtist in local_track.artists do
						local track_artist_id = track_artist.id
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
						if local_track.id and mb_artist.id and not matched_mb_artist_ids[mb_artist.id] then
							local ensured_artist_id = ensure_artist_entity(mb_artist)
							if ensured_artist_id then
								tagged_artist_ids[ensured_artist_id] = true
								tagged_artist_mb_ids[ensured_artist_id] = mb_artist.id
								matched_mb_artist_ids[mb_artist.id] = true
								track_artist_count += 1
								maybe_fetch_artist_relations(ensured_artist_id, mb_artist)
							end
						end
					end
					for _, artist_id in
						artist_role_credits(
							mb_credit_list,
							tagged_artist_mb_ids,
							character_voice_artist_ids,
							artist_created_at_ms
						)
					do
						add_credit(track_credits, "artist", artist_id, nil)
					end
					credit_roles.artist = track_credits.artist ~= nil
				end
				set_credit_roles(track_layer, track_credits, credit_roles)
				track_layer:save()
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
				local updated_ids: { [string]: boolean } = {}
				local function apply_track_artist(entity: LocalArtist)
					if entity.id == nil or updated_ids[entity.id] then
						return
					end
					updated_ids[entity.id] = true
					if tag_local_artist(entity.id, mb_track_artist) then
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
