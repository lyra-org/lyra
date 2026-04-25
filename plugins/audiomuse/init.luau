--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local http = require("@harmony/http")
local ids = require("@lyra/ids")
local json = require("@harmony/json")
local mix = require("@lyra/mix")
local plugins = require("@lyra/plugins")
local tracks = require("@lyra/tracks")

local jellyfin_hex = require("@self/jellyfin_hex")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse
type MixContext = mix.MixContext
type MixRecentListensContext = mix.MixRecentListensContext
type MixResult = mix.MixResult
type MixResultTrack = mix.MixResultTrack

local config = plugins.declare_settings(function(builder)
	builder:group("connection", "Connection")
	builder:string("base_url", {
		label = "Base URL",
		description = "AudioMuse server base URL.",
		default = "",
	})
	builder:string("token", {
		label = "Token",
		description = "Authentication token for the AudioMuse API.",
		default = "",
	})
end)

local BASE_URL = config.base_url :: string
local TOKEN = config.token :: string

if not BASE_URL or BASE_URL == "" then
	print("base_url not configured; skipping initialization.")
	return
end

http.set_rate_limit({
	domain = BASE_URL,
	requests_per_second = 10,
})

local function auth_headers(): { [string]: string }
	if TOKEN and TOKEN ~= "" then
		return { Authorization = `Bearer {TOKEN}` }
	end
	return {}
end

-- AudioMuse's Flask endpoints require `Content-Type: application/json`.
local function json_post_headers(): { [string]: string }
	local headers = auth_headers()
	headers["Content-Type"] = "application/json"
	return headers
end

local function safe_json_decode(raw: string): JsonValue?
	local ok, value = pcall(json.decode, raw)
	if not ok then
		return nil
	end
	return value
end

local function resolve_hex_id(db_id: number): string?
	local nanoid = ids.get_id(db_id)
	if not nanoid then
		return nil
	end
	return jellyfin_hex.nanoid_to_hex(nanoid)
end

local function query_similar_tracks(item_id: string, n: number): { MixResultTrack }?
	local url = `{BASE_URL}/api/similar_tracks?item_id={item_id}&n={n}`
	local response: HttpResponse = http.request({
		url = url,
		method = http.HttpMethod.Get,
		headers = auth_headers(),
	})

	if not response.success then
		-- 404 = seed not yet analyzed; distinct from a backend outage.
		if response.status_code == 404 then
			return {}
		end
		print(
			`similar_tracks({item_id}) backend failure: status={response.status_code} kind={response.error_kind or ""}`
		)
		return nil
	end

	local body = safe_json_decode(response.body)
	if type(body) ~= "table" then
		print(`similar_tracks({item_id}) returned non-JSON or non-object body`)
		return nil
	end

	local items = body :: { JsonObject }
	local results: { MixResultTrack } = {}
	local decode_failed = 0
	local orphaned = 0
	for _, item in items do
		if type(item) ~= "table" then
			decode_failed += 1
			continue
		end
		local result_item_id = item.item_id :: string?
		if type(result_item_id) ~= "string" then
			decode_failed += 1
			continue
		end
		local nanoid = jellyfin_hex.hex_to_nanoid(result_item_id)
		if not nanoid then
			decode_failed += 1
			continue
		end
		local track_db_id = ids.get_db_id(nanoid)
		if not track_db_id then
			orphaned += 1
			continue
		end
		table.insert(results, { track_id = track_db_id })
	end

	-- Encoder/version mismatch; escalate above routine orphans.
	if decode_failed > 0 then
		print(
			`[error] similar_tracks({item_id}): {decode_failed}/{#items} hex decode failed — possible ID format mismatch`
		)
	end
	if orphaned > 0 then
		print(`similar_tracks({item_id}): {#items} returned, {#results} resolved, {orphaned} orphaned`)
	end

	return results
end

-- Queries similarity for multiple seed track DB IDs, merges and deduplicates.
local function multi_seed_mix(seed_track_db_ids: { number }, limit: number, max_seeds: number): MixResult
	if #seed_track_db_ids == 0 then
		return { tracks = {} }
	end

	local all_results: { MixResultTrack } = {}
	local seen: { [number]: boolean } = {}
	-- Pre-mark seed tracks so they're excluded from results
	for _, seed_id in seed_track_db_ids do
		seen[seed_id] = true
	end
	local seeds_to_use = math.min(#seed_track_db_ids, max_seeds)
	-- AudioMuse's top-K for a within-scope seed often overlaps the other seeds; request
	-- `limit + #seeds` per query so we keep `limit` survivors after the seen[] filter.
	local per_seed = limit + #seed_track_db_ids
	local failed = 0

	for i = 1, seeds_to_use do
		if #all_results >= limit then
			break
		end
		local track_db_id = seed_track_db_ids[i]
		local hex_id = resolve_hex_id(track_db_id)
		if not hex_id then
			continue
		end
		local results = query_similar_tracks(hex_id, per_seed)
		if results then
			for _, result in results do
				if not seen[result.track_id] then
					seen[result.track_id] = true
					table.insert(all_results, result)
					if #all_results >= limit then
						break
					end
				end
			end
		else
			failed += 1
		end
	end

	if failed > 0 then
		print(`{failed}/{seeds_to_use} seed queries failed`)
	end

	return { tracks = all_results }
end

-- Register the mixer
local mixer = mix.Mixer.new("audiomuse")

mixer:from_track(function(ctx: MixContext): MixResult
	local hex_id = resolve_hex_id(ctx.seed_id)
	if not hex_id then
		print(`from_track: seed {ctx.seed_id} could not be encoded; returning empty mix`)
		return { tracks = {} }
	end

	local limit = ctx.limit or 50
	local results = query_similar_tracks(hex_id, limit + 1)
	if not results then
		return { tracks = {} }
	end

	-- Filter out the seed track from results
	local filtered: { MixResultTrack } = {}
	for _, result in results do
		if result.track_id ~= ctx.seed_id then
			table.insert(filtered, result)
		end
	end

	return { tracks = filtered }
end)

mixer:from_release(function(ctx: MixContext): MixResult
	local release_tracks = tracks.list(ctx.seed_id)
	if not release_tracks or #release_tracks == 0 then
		print(`from_release: seed {ctx.seed_id} has no tracks; returning empty mix`)
		return { tracks = {} }
	end

	local seed_ids: { number } = {}
	for _, track in release_tracks do
		if track.db_id then
			table.insert(seed_ids, track.db_id)
		end
	end

	return multi_seed_mix(seed_ids, ctx.limit or 50, 3)
end)

mixer:from_artist(function(ctx: MixContext): MixResult
	-- tracks.list lacks the artist→track edge traversal; use the artist-scoped query API.
	local result = tracks.query({ artist_ids = { ctx.seed_id } })
	local artist_tracks = result.entities
	if #artist_tracks == 0 then
		print(`from_artist: seed {ctx.seed_id} has no tracks; returning empty mix`)
		return { tracks = {} }
	end

	local seed_ids: { number } = {}
	for _, track in artist_tracks do
		if track.db_id then
			table.insert(seed_ids, track.db_id)
		end
	end

	return multi_seed_mix(seed_ids, ctx.limit or 50, 3)
end)

mixer:from_recent_listens(function(ctx: MixRecentListensContext): MixResult
	local recent_ids = ctx.recent_track_ids
	if not recent_ids or #recent_ids == 0 then
		return { tracks = {} }
	end

	return multi_seed_mix(recent_ids, ctx.limit or 50, 5)
end)

-- Operational routes (separate from mix provider)

api.post("/audiomuse/analysis/start", function(ctx: ApiContext): ApiResponse
	-- Forward the client body so callers can scope the analysis (e.g. num_recent_albums).
	local request_body = ctx.request.body_raw
	if type(request_body) ~= "string" or request_body == "" then
		request_body = "{}"
	end

	local response: HttpResponse = http.request({
		url = `{BASE_URL}/api/analysis/start`,
		method = http.HttpMethod.Post,
		headers = json_post_headers(),
		body = request_body,
	})

	local body = safe_json_decode(response.body)
	local status = response.status_code or 502
	-- 2xx with an `error` payload is a gateway failure.
	if status < 400 and type(body) == "table" and type((body :: JsonObject).error) == "string" then
		return api.response.json(502, body)
	end

	return api.response.json(status, body or {
		error = `analysis start failed: {response.status_code or 0}`,
	})
end)

api.get("/audiomuse/status/{task_id}", function(ctx: ApiContext): ApiResponse
	local task_id = ctx.params.task_id
	if not task_id or not string.match(task_id, "^[%w%-_]+$") then
		return api.response.json(400, { error = "invalid task_id" })
	end

	local response: HttpResponse = http.request({
		url = `{BASE_URL}/api/status/{task_id}`,
		method = http.HttpMethod.Get,
		headers = auth_headers(),
	})

	local body = safe_json_decode(response.body)
	return api.response.json(response.status_code or 502, body or {})
end)

api.get("/audiomuse/health", function(): ApiResponse
	local response: HttpResponse = http.request({
		url = `{BASE_URL}/`,
		method = http.HttpMethod.Get,
	})

	return api.response.json(response.status_code or 502, {
		healthy = response.success,
		backend_url = BASE_URL,
	})
end)

print("initialized with backend at " .. BASE_URL)
