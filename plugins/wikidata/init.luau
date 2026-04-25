--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local genres = require("@lyra/genres")
local http = require("@harmony/http")
local json = require("@harmony/json")
local metadata = require("@lyra/metadata")
local plugins = require("@lyra/plugins")

local ET = metadata.EntityType
local provider = metadata.Provider.new("wikidata")

local WIKIDATA_API = "https://www.wikidata.org/w/api.php"
local CACHE_STORE_NAME = "wikidata"
local GENRE_CACHE_TTL = 30 * 24 * 60 * 60 -- 30 days for genre details
local RELEASE_HIT_CACHE_TTL = 30 * 24 * 60 * 60 -- 30 days for successful matches
local RELEASE_MISS_CACHE_TTL = 7 * 24 * 60 * 60 -- 7 days for misses (allows recovery)
local MAX_GENRES_PER_RELEASE = 20

local config = plugins.declare_settings(function(builder)
	builder:group("hierarchy", "Genre Hierarchy")
	builder:number("hierarchy_depth", {
		label = "Hierarchy depth",
		description = "How many levels of parent genres to resolve from WikiData P279 (subclass of) claims. 0 disables hierarchy. Higher values produce more abstract ancestor genres.",
		default = 0,
		min = 0,
		max = 5,
	})
end)

local HIERARCHY_DEPTH = (config.hierarchy_depth or 2) :: number

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type DataStore = datastore.DataStore

local cache_store: DataStore? = nil

-- Wikimedia allows ~1000 req/hr for unauthenticated clients with a compliant
-- User-Agent (set server-wide via outbound_user_agent). 0.2 req/s = 720 req/hr,
-- well within the hourly quota.
http.set_rate_limit({
	domain = "www.wikidata.org",
	requests_per_second = 0.2,
	retry_on = { 429, 503 },
	max_retries = 3,
	backoff_ms = 5000,
})

provider:id({
	id = "qid",
	entity = "release",
}, function(id)
	return `https://www.wikidata.org/wiki/{id}`
end)

local function as_json_object(value: JsonValue): JsonObject?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "{" then
		return nil
	end
	return value :: JsonObject
end

local function as_json_array(value: JsonValue): JsonArray?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "[" then
		return nil
	end
	return value :: JsonArray
end

local function json_string_field(value: JsonObject?, key: string): string?
	local field = if value then value[key] else nil
	if type(field) == "string" and field ~= "" then
		return field
	end
	return nil
end

local function is_valid_qid(value: string?): boolean
	if not value then
		return false
	end
	return string.match(value, "^Q%d+$") ~= nil
end

local function get_cache_store(): DataStore?
	if cache_store ~= nil then
		return cache_store
	end
	local ok, store = pcall(datastore.get_or_create, CACHE_STORE_NAME)
	if not ok then
		return nil
	end
	cache_store = store
	return store
end

local function cache_get(key: string): JsonObject?
	local store = get_cache_store()
	if not store then
		return nil
	end
	local ok, raw = pcall(store.get, store, key)
	if not ok then
		return nil
	end
	local entry = as_json_object(raw)
	if not entry then
		return nil
	end
	local expires_at = entry.expires_at
	if type(expires_at) ~= "number" or expires_at <= os.time() then
		return nil
	end
	return as_json_object(entry.data)
end

local function cache_set(key: string, data: JsonValue, ttl: number)
	local store = get_cache_store()
	if not store then
		return
	end
	pcall(
		store.set,
		store,
		key,
		{
			data = data,
			expires_at = os.time() + ttl,
		} :: JsonValue
	)
end

local function wikidata_get(params: string): JsonObject?
	local request: HttpRequestOptions = {
		url = `{WIKIDATA_API}?{params}&format=json`,
		method = http.HttpMethod.Get,
	}

	local ok, response = pcall(http.request, request)
	if not ok or type(response) ~= "table" then
		return nil
	end
	if not response.success or response.status_code ~= 200 then
		return nil
	end

	local dec_ok, data = pcall(json.decode, response.body :: string)
	if not dec_ok then
		return nil
	end

	return as_json_object(data)
end

local function extract_claim_qid(claim: JsonValue): string?
	local claim_obj = as_json_object(claim)
	if not claim_obj then
		return nil
	end
	local mainsnak = as_json_object(claim_obj.mainsnak)
	if not mainsnak then
		return nil
	end
	local datavalue = as_json_object(mainsnak.datavalue)
	if not datavalue then
		return nil
	end
	local value = as_json_object(datavalue.value)
	if not value then
		return nil
	end
	local qid = json_string_field(value, "id")
	if not is_valid_qid(qid) then
		return nil
	end
	return qid
end

local function extract_genre_qids(claims: JsonObject): { string }
	local genre_claims = as_json_array(claims.P136)
	if not genre_claims then
		return {}
	end

	local qids: { string } = {}
	for _, claim in genre_claims do
		if #qids >= MAX_GENRES_PER_RELEASE then
			break
		end
		local qid = extract_claim_qid(claim)
		if qid then
			table.insert(qids, qid)
		end
	end

	return qids
end

-- Album type QIDs: album, studio album, compilation album, EP, single, live album, remix album.
local ALBUM_TYPES: { [string]: boolean } = {
	Q482994 = true,
	Q209939 = true,
	Q222910 = true,
	Q1060742 = true,
	Q169930 = true,
	Q209945 = true,
	Q15982858 = true,
}

local function is_album_entity(claims: JsonObject): boolean
	local p31_claims = as_json_array(claims.P31)
	if not p31_claims then
		return false
	end
	for _, claim in p31_claims do
		local type_qid = extract_claim_qid(claim)
		if type_qid and ALBUM_TYPES[type_qid] then
			return true
		end
	end
	return false
end

local function collect_performer_qids(release_candidates: { { qid: string, claims: JsonObject } }): { string }
	local seen: { [string]: boolean } = {}
	local qids: { string } = {}
	for _, candidate in release_candidates do
		local p175_claims = as_json_array(candidate.claims.P175)
		if p175_claims then
			for _, claim in p175_claims do
				local performer_qid = extract_claim_qid(claim)
				if performer_qid and not seen[performer_qid] then
					seen[performer_qid] = true
					table.insert(qids, performer_qid)
				end
			end
		end
	end
	return qids
end

-- Returns nil on API failure (distinct from empty table = no performers found).
local function fetch_performer_names(performer_qids: { string }): { [string]: { string } }?
	if #performer_qids == 0 then
		return {}
	end

	local names: { [string]: { string } } = {}

	for batch_start = 1, #performer_qids, 50 do
		local batch_end = math.min(batch_start + 49, #performer_qids)
		local batch_ids: { string } = {}
		for i = batch_start, batch_end do
			table.insert(batch_ids, performer_qids[i])
		end

		local ids_param = table.concat(batch_ids, "|")
		local performers_data = wikidata_get(`action=wbgetentities&ids={ids_param}&props=labels|aliases&languages=en`)
		if not performers_data then
			return nil
		end

		local entities = as_json_object(performers_data.entities)
		if not entities then
			return nil
		end

		for _, qid in batch_ids do
			local entity = as_json_object(entities[qid])
			if not entity then
				continue
			end

			local entity_names: { string } = {}

			local labels = as_json_object(entity.labels)
			local en_label_obj = if labels then as_json_object(labels.en) else nil
			local label = json_string_field(en_label_obj, "value")
			if label then
				table.insert(entity_names, string.lower(label))
			end

			local all_aliases = as_json_object(entity.aliases)
			local en_aliases = if all_aliases then as_json_array(all_aliases.en) else nil
			if en_aliases then
				for _, alias_entry in en_aliases do
					local alias_obj = as_json_object(alias_entry)
					local alias_value = json_string_field(alias_obj, "value")
					if alias_value then
						table.insert(entity_names, string.lower(alias_value))
					end
				end
			end

			names[qid] = entity_names
		end
	end

	return names
end

local function candidate_matches_artist(
	claims: JsonObject,
	artist_name: string?,
	performer_names: { [string]: { string } }
): boolean
	if not artist_name or artist_name == "" then
		return true
	end

	local p175_claims = as_json_array(claims.P175)
	if not p175_claims then
		-- No performer claims — can't confirm or deny, accept the match.
		return true
	end

	local lower_artist = string.lower(artist_name)

	for _, claim in p175_claims do
		local performer_qid = extract_claim_qid(claim)
		if performer_qid then
			local names = performer_names[performer_qid]
			if names then
				for _, name in names do
					if name == lower_artist then
						return true
					end
				end
			end
		end
	end

	return false
end

type ReleaseSearchResult = {
	qid: string,
	genre_qids: { string },
}

-- Uses title-only wbsearchentities to avoid injection, returns genre QIDs
-- alongside the match to avoid a redundant re-fetch of the entity's claims.
local function search_release(release_title: string, artist_name: string?): ReleaseSearchResult?
	-- Search by title only — wbsearchentities does label matching, not structured search.
	local search_params =
		`action=wbsearchentities&search={http.encode_uri_component(release_title)}&language=en&type=item&limit=10`
	local search_data = wikidata_get(search_params)
	if not search_data then
		return nil
	end

	local search_results = as_json_array(search_data.search)
	if not search_results or #search_results == 0 then
		return nil
	end

	-- Collect candidate QIDs with format validation.
	local candidate_qids: { string } = {}
	for _, result in search_results do
		local result_obj = as_json_object(result)
		local qid = json_string_field(result_obj, "id")
		if is_valid_qid(qid) then
			table.insert(candidate_qids, qid :: string)
		end
	end

	if #candidate_qids == 0 then
		return nil
	end

	-- Batch-fetch candidates — get claims for type checking, performer matching,
	-- and genre extraction all in one call.
	local ids_param = table.concat(candidate_qids, "|")
	local entities_data = wikidata_get(`action=wbgetentities&ids={ids_param}&props=claims`)
	if not entities_data then
		return nil
	end

	local entities = as_json_object(entities_data.entities)
	if not entities then
		return nil
	end

	-- First pass: identify album-type candidates and collect their claims.
	local release_candidates: { { qid: string, claims: JsonObject } } = {}
	for _, qid in candidate_qids do
		local entity = as_json_object(entities[qid])
		if not entity then
			continue
		end
		local claims = as_json_object(entity.claims)
		if not claims then
			continue
		end
		if is_album_entity(claims) then
			table.insert(release_candidates, { qid = qid, claims = claims })
		end
	end

	if #release_candidates == 0 then
		return nil
	end

	-- Batch-fetch all performer labels across all candidates in one API call.
	local all_performer_qids = collect_performer_qids(release_candidates)
	local performer_names = fetch_performer_names(all_performer_qids)

	-- If performer fetch failed and we have an artist filter, skip all candidates.
	-- Wrong data is harder to recover from than missing data.
	if performer_names == nil and artist_name and artist_name ~= "" then
		return nil
	end

	-- Second pass: find the first candidate matching the artist.
	for _, candidate in release_candidates do
		if candidate_matches_artist(candidate.claims, artist_name, performer_names or {}) then
			-- Extract genre QIDs from the same claims we already have.
			local genre_qids = extract_genre_qids(candidate.claims)
			return {
				qid = candidate.qid,
				genre_qids = genre_qids,
			}
		end
	end

	return nil
end

type GenreClaim = {
	qid: string,
	label: string,
	aliases: { string },
	parent_qids: { string },
}

local function fetch_genre_details(qids: { string }): { GenreClaim }
	if #qids == 0 then
		return {}
	end

	local results: { GenreClaim } = {}

	-- Batch in groups of 50 (API limit).
	for batch_start = 1, #qids, 50 do
		local batch_end = math.min(batch_start + 49, #qids)
		local batch_ids: { string } = {}
		for i = batch_start, batch_end do
			table.insert(batch_ids, qids[i])
		end

		local uncached_ids: { string } = {}

		for _, qid in batch_ids do
			local cached = cache_get(`genre:{qid}`)
			if cached then
				local label = json_string_field(cached, "label")
				if label then
					local aliases_raw = as_json_array(cached.aliases)
					local aliases: { string } = {}
					if aliases_raw then
						for _, a in aliases_raw do
							if type(a) == "string" then
								table.insert(aliases, a)
							end
						end
					end
					local parent_qids_raw = as_json_array(cached.parent_qids)
					local parent_qids: { string } = {}
					if parent_qids_raw then
						for _, p in parent_qids_raw do
							if type(p) == "string" then
								table.insert(parent_qids, p)
							end
						end
					end
					table.insert(results, {
						qid = qid,
						label = label,
						aliases = aliases,
						parent_qids = parent_qids,
					})
				end
			else
				table.insert(uncached_ids, qid)
			end
		end

		if #uncached_ids > 0 then
			local fetch_ids_param = table.concat(uncached_ids, "|")
			local api_root =
				wikidata_get(`action=wbgetentities&ids={fetch_ids_param}&languages=en&props=labels|aliases|claims`)
			if not api_root then
				continue
			end

			local entities = as_json_object(api_root.entities)
			if not entities then
				continue
			end

			for _, qid in uncached_ids do
				local entity = as_json_object(entities[qid])
				if not entity then
					continue
				end

				local labels = as_json_object(entity.labels)
				local en_label_obj = if labels then as_json_object(labels.en) else nil
				local label = json_string_field(en_label_obj, "value")
				if not label then
					continue
				end

				local aliases: { string } = {}
				local all_aliases = as_json_object(entity.aliases)
				local en_aliases = if all_aliases then as_json_array(all_aliases.en) else nil
				if en_aliases then
					for _, alias_entry in en_aliases do
						local alias_obj = as_json_object(alias_entry)
						local alias_value = json_string_field(alias_obj, "value")
						if alias_value then
							table.insert(aliases, alias_value)
						end
					end
				end

				-- Extract P279 (subclass of) parent genre QIDs.
				local parent_qids: { string } = {}
				local entity_claims = as_json_object(entity.claims)
				if entity_claims then
					local p279_claims = as_json_array(entity_claims.P279)
					if p279_claims then
						for _, p279_claim in p279_claims do
							local parent_qid = extract_claim_qid(p279_claim)
							if parent_qid then
								table.insert(parent_qids, parent_qid)
							end
						end
					end
				end

				local claim: GenreClaim = {
					qid = qid,
					label = label,
					aliases = aliases,
					parent_qids = parent_qids,
				}
				table.insert(results, claim)

				-- Cache the result.
				local cache_aliases: { JsonValue } = {}
				for _, a in aliases do
					table.insert(cache_aliases, a :: JsonValue)
				end
				local cache_parents: { JsonValue } = {}
				for _, p in parent_qids do
					table.insert(cache_parents, p :: JsonValue)
				end
				cache_set(
					`genre:{qid}`,
					{
						label = label,
						aliases = cache_aliases :: JsonValue,
						parent_qids = cache_parents :: JsonValue,
					} :: JsonValue,
					GENRE_CACHE_TTL
				)
			end
		end
	end

	return results
end

local function get_artist_name(ctx: metadata.ReleaseRefreshContext): string?
	if ctx.lookup_hints then
		local hints = ctx.lookup_hints :: { artist_name: string?, release_title: string?, year: number? }
		if hints.artist_name and hints.artist_name ~= "" then
			return hints.artist_name
		end
	end
	if ctx.artists then
		local artists = ctx.artists :: { { db_id: number?, artist_name: string } }
		if #artists > 0 and artists[1].artist_name ~= "" then
			return artists[1].artist_name
		end
	end
	return nil
end

provider:refresh(ET.Release, function(ctx: metadata.ReleaseRefreshContext)
	local release_title = ctx.release_title
	local release_db_id = ctx.db_id
	if not release_title or release_title == "" or not release_db_id then
		return nil
	end

	local artist_name = get_artist_name(ctx)

	-- Cache key includes normalized title to avoid stale data if the DB ID is reused.
	-- Normalize: trim whitespace, lowercase — resilient to cosmetic tag edits.
	local normalized_title = string.lower((string.gsub(release_title, "^%s+", ""):gsub("%s+$", "")))
	local cache_key = `release:{release_db_id}:{normalized_title}`
	local cached = cache_get(cache_key)
	if cached then
		return nil
	end

	-- Step 1
	local result = search_release(release_title, artist_name)
	if not result then
		cache_set(cache_key, { miss = true :: JsonValue } :: JsonValue, RELEASE_MISS_CACHE_TTL)
		return nil
	end

	local release_qid = result.qid
	local genre_qids = result.genre_qids

	if #genre_qids == 0 then
		-- No genre claims, but still store the QID so we don't re-search.
		local release_layer = provider:layer(release_db_id)
		release_layer:set_id("qid", release_qid)
		release_layer:save()
		cache_set(
			cache_key,
			{ qid = release_qid :: JsonValue, genres = 0 :: JsonValue } :: JsonValue,
			RELEASE_HIT_CACHE_TTL
		)
		return nil
	end

	-- Step 2
	local genre_details = fetch_genre_details(genre_qids)
	if #genre_details == 0 then
		local release_layer = provider:layer(release_db_id)
		release_layer:set_id("qid", release_qid)
		release_layer:save()
		cache_set(
			cache_key,
			{ qid = release_qid :: JsonValue, genres = 0 :: JsonValue } :: JsonValue,
			RELEASE_HIT_CACHE_TTL
		)
		return nil
	end

	-- Step 3: Register genres via genres.add() with pcall to avoid partial writes.
	-- genres.add() is idempotent — re-adding an existing genre is a no-op via
	-- the resolve() lookup (matches by external ID or name).
	local genre_names: { JsonValue } = {}
	local genre_db_ids: { [string]: number } = {} -- QID → db_id mapping
	local all_genres_ok = true
	local GENRE_SUFFIXES = { " music", " song" }

	for _, genre in genre_details do
		local alias_inputs: { { name: string, locale: string? } } = {}
		for _, alias in genre.aliases do
			table.insert(alias_inputs, { name = alias, locale = "en" })
		end

		-- Strip known suffixes from verbose WikiData labels so the alias
		-- lookup finds existing short-named genres.  layer_name tracks the
		-- resolved form for the metadata layer — using the verbose label
		-- there would cause sync_release_genres to re-create the duplicate.
		local layer_name = genre.label
		local lower_label = string.lower(genre.label)
		for _, suffix in GENRE_SUFFIXES do
			if string.sub(lower_label, -#suffix) == suffix then
				local stripped = string.sub(genre.label, 1, #genre.label - #suffix)
				if #stripped > 0 then
					table.insert(alias_inputs, { name = stripped, locale = "en" })
					layer_name = stripped
				end
			end
		end

		local add_ok, genre_db_id = pcall(genres.add, release_db_id, {
			name = genre.label,
			external_id = {
				provider_id = "wikidata",
				id_type = "qid",
				id = genre.qid,
			},
			aliases = alias_inputs,
		})

		if add_ok and genre_db_id then
			table.insert(genre_names, layer_name :: JsonValue)
			genre_db_ids[genre.qid] = genre_db_id
		else
			all_genres_ok = false
		end
	end

	-- Step 4: Resolve parent genres and link hierarchy (P279 subclass of).
	-- Depth is configurable via plugin settings (default 1). Higher values
	-- walk further up the WikiData taxonomy. 0 disables hierarchy entirely.
	-- Each iteration resolves the parents of the most recently fetched level.
	local all_fetched_genres: { GenreClaim } = {}
	for _, g in genre_details do
		table.insert(all_fetched_genres, g)
	end

	local current_level = genre_details
	for _depth = 1, HIERARCHY_DEPTH do
		-- Collect parent QIDs from the current level that aren't already resolved.
		local parent_qids_to_resolve: { string } = {}
		local seen_parent_qids: { [string]: boolean } = {}
		for _, genre in current_level do
			for _, parent_qid in genre.parent_qids do
				if not genre_db_ids[parent_qid] and not seen_parent_qids[parent_qid] then
					seen_parent_qids[parent_qid] = true
					table.insert(parent_qids_to_resolve, parent_qid)
				end
			end
		end

		if #parent_qids_to_resolve == 0 then
			break
		end

		-- Fetch and register parent genres (not linked to this release).
		local parent_details = fetch_genre_details(parent_qids_to_resolve)
		for _, parent in parent_details do
			local parent_alias_inputs: { { name: string, locale: string? } } = {}
			for _, alias in parent.aliases do
				table.insert(parent_alias_inputs, { name = alias, locale = "en" })
			end
			local lower_parent = string.lower(parent.label)
			for _, suffix in GENRE_SUFFIXES do
				if string.sub(lower_parent, -#suffix) == suffix then
					local stripped = string.sub(parent.label, 1, #parent.label - #suffix)
					if #stripped > 0 then
						table.insert(parent_alias_inputs, { name = stripped, locale = "en" })
					end
				end
			end

			local parent_request: genres.GenreAddRequest = {
				name = parent.label,
				external_id = {
					provider_id = "wikidata",
					id_type = "qid",
					id = parent.qid,
				},
				aliases = parent_alias_inputs,
			}
			local parent_ok, parent_db_id = pcall(genres.resolve, parent_request)
			if parent_ok and parent_db_id then
				genre_db_ids[parent.qid] = parent_db_id
			end
			table.insert(all_fetched_genres, parent)
		end

		-- Next iteration walks parents of this level's parents.
		current_level = parent_details
	end

	-- Link child → parent relationships across all fetched levels.
	for _, genre in all_fetched_genres do
		local child_db_id = genre_db_ids[genre.qid]
		if not child_db_id then
			continue
		end
		for _, parent_qid in genre.parent_qids do
			local parent_db_id = genre_db_ids[parent_qid]
			if parent_db_id then
				pcall(genres.add_parent, child_db_id, parent_db_id)
			end
		end
	end

	-- Save layer and cache only when all genres succeeded. If some failed,
	-- don't write the QID — the sync filter checks external_ids.qid, so
	-- writing it would permanently skip the release and prevent retry.
	if all_genres_ok and #genre_names > 0 then
		local release_layer = provider:layer(release_db_id)
		release_layer:set_field("genres", genre_names)
		release_layer:set_id("qid", release_qid)
		release_layer:save()
		local genre_count: JsonValue = #genre_names
		cache_set(
			cache_key,
			{ qid = release_qid :: JsonValue, genres = genre_count } :: JsonValue,
			RELEASE_HIT_CACHE_TTL
		)
	end

	return nil
end, function(ctx: metadata.ReleaseRefreshContext): boolean
	-- Skip releases that already have a WikiData QID — either from us ("qid")
	-- or pre-populated by MusicBrainz cooperation ("wikidata_qid").
	-- Once a QID is in external_ids, this release is permanently skipped.
	-- A wrong QID requires manual correction (no automatic recovery path).
	if ctx.external_ids then
		local ids = ctx.external_ids :: { [string]: string }
		if (ids.qid and ids.qid ~= "") or (ids.wikidata_qid and ids.wikidata_qid ~= "") then
			return false
		end
	end
	return true
end)
