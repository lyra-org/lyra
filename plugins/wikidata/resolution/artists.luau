--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Artist descriptions: WikiData holds no prose, so the entity's Wikipedia
-- sitelink is resolved and the article intro fetched from that wiki.

local http = require("@harmony/http")
local libraries = require("@lyra/libraries")
local locale = require("@lyra/locale")
local metadata = require("@lyra/metadata")
local shared = require("./shared")

type JsonValue = shared.JsonValue
type JsonObject = shared.JsonObject

local as_json_object = shared.as_json_object
local as_json_array = shared.as_json_array
local json_string_field = shared.json_string_field
local is_valid_qid = shared.is_valid_qid
local trim = shared.trim
local wikidata_get = shared.wikidata_get
local extract_claim_qid = shared.extract_claim_qid
local extract_claim_string = shared.extract_claim_string

-- `site` is the Wikipedia subdomain and sitelink prefix; `term` is the
-- WikiData language code for labels, descriptions, and search.
type WikiLanguage = {
	site: string,
	term: string,
	iso3: string,
}

-- Languages whose Wikimedia codes differ from ISO 639-1, keyed by ISO 639-3.
local WIKI_CODES: { [string]: { site: string, term: string } } = {
	nob = { site = "no", term = "nb" },
	nor = { site = "no", term = "nb" },
}

local ENGLISH: WikiLanguage = { site = "en", term = "en", iso3 = "eng" }

-- Artist entity types: human, musical group, rock band, musical ensemble,
-- all-female band, girl group, boy band, musical duo.
local ARTIST_TYPES: { [string]: boolean } = {
	Q5 = true,
	Q215380 = true,
	Q5741069 = true,
	Q2088357 = true,
	Q7623897 = true,
	Q641066 = true,
	Q216337 = true,
	Q9212979 = true,
}

local wikipedia_hosts_with_rate_limit: { [string]: boolean } = {}

local function ensure_wikipedia_rate_limit(host: string)
	if wikipedia_hosts_with_rate_limit[host] then
		return
	end
	wikipedia_hosts_with_rate_limit[host] = true
	http.set_rate_limit({
		domain = host,
		requests_per_second = shared.RATE_LIMIT.requests_per_second,
		retry_on = shared.RATE_LIMIT.retry_on,
		max_retries = shared.RATE_LIMIT.max_retries,
		backoff_ms = shared.RATE_LIMIT.backoff_ms,
	})
end

-- Lowest library db_id with a resolvable language, matching the server's
-- library selection for shared entities.
local function library_language(artist_db_id: number): WikiLanguage?
	local ok, artist_libraries = pcall(libraries.get_for_entity, artist_db_id)
	if not ok then
		return nil
	end
	local chosen: WikiLanguage? = nil
	local chosen_db_id: number? = nil
	for _, library in artist_libraries do
		local language = locale.language(library.language)
		if not language or not library.db_id then
			continue
		end
		if chosen_db_id and library.db_id >= chosen_db_id then
			continue
		end
		local codes = WIKI_CODES[language.iso3]
		local site = if codes then codes.site else language.iso2
		local term = if codes then codes.term else language.iso2
		if site and term then
			chosen = { site = site, term = term, iso3 = language.iso3 }
			chosen_db_id = library.db_id
		end
	end
	return chosen
end

local function preferred_languages(artist_db_id: number): { WikiLanguage }
	local preferred = library_language(artist_db_id)
	if not preferred or preferred.site == ENGLISH.site then
		return { ENGLISH }
	end
	return { preferred, ENGLISH }
end

local function is_valid_mbid(value: string?): boolean
	if not value then
		return false
	end
	return string.match(value, "^%x%x%x%x%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$") ~= nil
end

local function musicbrainz_artist_id(ctx: metadata.ArtistRefreshContext): string?
	local musicbrainz_ids = metadata.ids.for_provider(ctx.external_ids, "musicbrainz")
	local artist_id = if musicbrainz_ids then musicbrainz_ids.artist_id else nil
	if is_valid_mbid(artist_id) then
		return string.lower(artist_id :: string)
	end
	return nil
end

local function is_artist_entity(claims: JsonObject): boolean
	local p31_claims = as_json_array(claims.P31)
	if not p31_claims then
		return false
	end
	for _, claim in p31_claims do
		local type_qid = extract_claim_qid(claim)
		if type_qid and ARTIST_TYPES[type_qid] then
			return true
		end
	end
	return false
end

local function entity_has_mbid(claims: JsonObject, mbid: string): boolean
	local p434_claims = as_json_array(claims.P434)
	if not p434_claims then
		return false
	end
	for _, claim in p434_claims do
		local value = extract_claim_string(claim)
		if value and string.lower(value) == mbid then
			return true
		end
	end
	return false
end

local function entity_label_matches(entity: JsonObject, lower_name: string): boolean
	local labels = as_json_object(entity.labels)
	if not labels then
		return false
	end
	for _, label in labels do
		local value = json_string_field(as_json_object(label), "value")
		if value and string.lower(value) == lower_name then
			return true
		end
	end
	return false
end

local function entity_props(languages: { WikiLanguage }): string
	local sites: { string } = {}
	local terms: { string } = {}
	for _, language in languages do
		table.insert(sites, `{language.site}wiki`)
		table.insert(terms, language.term)
	end
	return `sitefilter={table.concat(sites, "|")}&languages={table.concat(terms, "|")}`
end

-- Second return is true when the lookup failed rather than found nothing.
local function search_qid_by_mbid(mbid: string): (string?, boolean)
	local data = wikidata_get(`action=query&list=search&srsearch=haswbstatement:P434={mbid}&srlimit=2`)
	if not data then
		return nil, true
	end
	local query = as_json_object(data.query)
	local results = if query then as_json_array(query.search) else nil
	if not results or #results ~= 1 then
		return nil, false
	end
	local qid = json_string_field(as_json_object(results[1]), "title")
	if is_valid_qid(qid) then
		return qid, false
	end
	return nil, false
end

local function fetch_entity(qid: string, languages: { WikiLanguage }): (JsonObject?, boolean)
	local data = wikidata_get(`action=wbgetentities&ids={qid}&props=sitelinks|descriptions&{entity_props(languages)}`)
	if not data then
		return nil, true
	end
	local entities = as_json_object(data.entities)
	local entity = if entities then as_json_object(entities[qid]) else nil
	return entity, false
end

type ArtistMatch = {
	qid: string,
	entity: JsonObject,
}

-- Candidates must be artist-type entities. With a known MusicBrainz ID only a
-- matching P434 is accepted; otherwise an exact label match is required and
-- more than one such candidate is treated as ambiguous.
local function search_by_name(artist_name: string, mbid: string?, languages: { WikiLanguage }): (ArtistMatch?, boolean)
	local search_data = wikidata_get(
		`action=wbsearchentities&search={http.encode_uri_component(artist_name)}&language={languages[1].term}&type=item&limit=10`
	)
	if not search_data then
		return nil, true
	end

	local candidate_qids: { string } = {}
	local search_results = as_json_array(search_data.search)
	if search_results then
		for _, result in search_results do
			local qid = json_string_field(as_json_object(result), "id")
			if is_valid_qid(qid) then
				table.insert(candidate_qids, qid :: string)
			end
		end
	end
	if #candidate_qids == 0 then
		return nil, false
	end

	local entities_data = wikidata_get(
		`action=wbgetentities&ids={table.concat(candidate_qids, "|")}&props=claims|labels|sitelinks|descriptions&{entity_props(
			languages
		)}`
	)
	local entities = if entities_data then as_json_object(entities_data.entities) else nil
	if not entities then
		return nil, true
	end

	local lower_name = string.lower(artist_name)
	local matches: { ArtistMatch } = {}
	for _, qid in candidate_qids do
		local entity = as_json_object(entities[qid])
		local claims = if entity then as_json_object(entity.claims) else nil
		if not entity or not claims or not is_artist_entity(claims) then
			continue
		end
		local accepted = if mbid then entity_has_mbid(claims, mbid) else entity_label_matches(entity, lower_name)
		if accepted then
			table.insert(matches, { qid = qid, entity = entity })
		end
	end

	if #matches == 1 then
		return matches[1], false
	end
	return nil, false
end

local function fetch_wikipedia_intro(site: string, title: string): string?
	local host = `{site}.wikipedia.org`
	ensure_wikipedia_rate_limit(host)

	local data = shared.api_get(
		`https://{host}/w/api.php?action=query&prop=extracts&exintro=1&explaintext=1&redirects=1&titles={http.encode_uri_component(
			title
		)}&format=json`
	)
	local query = if data then as_json_object(data.query) else nil
	local pages = if query then as_json_object(query.pages) else nil
	if not pages then
		return nil
	end
	for _, page in pages do
		local extract = json_string_field(as_json_object(page), "extract")
		if extract then
			local trimmed = trim(extract)
			if trimmed ~= "" then
				return trimmed
			end
		end
	end
	return nil
end

type ArtistDescription = {
	text: string,
	language: WikiLanguage,
}

-- Wikipedia intro in preference order, then the WikiData description as a
-- short-phrase fallback in the same order.
local function entity_description(entity: JsonObject, languages: { WikiLanguage }): ArtistDescription?
	local sitelinks = as_json_object(entity.sitelinks)
	for _, language in languages do
		local sitelink = if sitelinks then as_json_object(sitelinks[`{language.site}wiki`]) else nil
		local title = json_string_field(sitelink, "title")
		if title then
			local intro = fetch_wikipedia_intro(language.site, title)
			if intro then
				return { text = intro, language = language }
			end
		end
	end

	local descriptions = as_json_object(entity.descriptions)
	for _, language in languages do
		local description = if descriptions then as_json_object(descriptions[language.term]) else nil
		local value = json_string_field(description, "value")
		if value then
			return { text = value, language = language }
		end
	end

	return nil
end

local function resolve_artist(artist_name: string, mbid: string?, languages: { WikiLanguage }): (ArtistMatch?, boolean)
	if mbid then
		local qid, failed = search_qid_by_mbid(mbid)
		if failed then
			return nil, true
		end
		if qid then
			local entity, fetch_failed = fetch_entity(qid, languages)
			if fetch_failed or not entity then
				return nil, true
			end
			return { qid = qid, entity = entity }, false
		end
	end
	if artist_name == "" then
		return nil, false
	end
	return search_by_name(artist_name, mbid, languages)
end

local module = {}

function module.register(provider: metadata.Provider)
	provider:refresh(metadata.EntityType.Artist, function(ctx: metadata.ArtistRefreshContext)
		local artist_db_id = ctx.db_id
		if not artist_db_id then
			return nil
		end

		local artist_name = if ctx.artist_name then trim(ctx.artist_name) else ""
		local mbid = musicbrainz_artist_id(ctx)
		if artist_name == "" and not mbid then
			return nil
		end

		local cache_key = `artist:{artist_db_id}:{string.lower(artist_name)}`
		if shared.cache_get(cache_key) then
			return nil
		end

		local languages = preferred_languages(artist_db_id)
		local match, failed = resolve_artist(artist_name, mbid, languages)
		if failed then
			return nil
		end
		if not match then
			shared.cache_set(cache_key, { miss = true :: JsonValue } :: JsonValue, shared.MISS_CACHE_TTL)
			return nil
		end

		local description = entity_description(match.entity, languages)

		local artist_layer = provider:layer(artist_db_id)
		artist_layer:set_id("qid", match.qid)
		if description then
			artist_layer:set_field("description", description.text)
			artist_layer:set_custom_field("v1", "description_language", description.language.iso3)
		end
		artist_layer:save()

		shared.cache_set(
			cache_key,
			{ qid = match.qid :: JsonValue, described = (description ~= nil) :: JsonValue } :: JsonValue,
			if description then shared.HIT_CACHE_TTL else shared.SHORT_MISS_CACHE_TTL
		)
		return nil
	end, function(ctx: metadata.ArtistRefreshContext): boolean
		if ctx.locked then
			return false
		end
		local wikidata_ids = metadata.ids.for_provider(ctx.external_ids, "wikidata")
		if wikidata_ids and wikidata_ids.qid and wikidata_ids.qid ~= "" then
			return false
		end
		if ctx.artist_name and trim(ctx.artist_name) ~= "" then
			return true
		end
		return musicbrainz_artist_id(ctx) ~= nil
	end)
end

return module
