--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json
local mix = require("@lyra/mix")
local plugins = require("@lyra/plugins")
local tracks = require("@lyra/tracks")

local jellyfin_hex = require("@self/jellyfin_hex")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse
type MixBlendContext = mix.MixBlendContext
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
	builder:choice("media_server", {
		label = "Media server",
		description = "The API the AudioMuse server indexes Lyra through.",
		default = "jellyfin",
		options = {
			{ value = "jellyfin", label = "Jellyfin" },
			{ value = "opensubsonic", label = "OpenSubsonic" },
		},
	})
end)

local BASE_URL = config.base_url :: string
local TOKEN = config.token :: string
local MEDIA_SERVER = config.media_server :: string

if not BASE_URL or BASE_URL == "" then
	warn("base_url not configured; skipping initialization.")
	return
end

if plugins.get(MEDIA_SERVER) == nil then
	warn(`media server plugin {MEDIA_SERVER} is not installed; skipping initialization.`)
	return
end

-- The track ids AudioMuse stores: Jellyfin item ids are hex encodings of Lyra ids, while
-- OpenSubsonic song ids are Lyra ids.
local function passthrough_id(id: string): string?
	return id
end
local encode_track_id = if MEDIA_SERVER == "jellyfin" then jellyfin_hex.nanoid_to_hex else passthrough_id
local decode_track_id = if MEDIA_SERVER == "jellyfin" then jellyfin_hex.hex_to_nanoid else passthrough_id

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

-- Whether AudioMuse answered with a 2xx status; `success` alone only means a response arrived.
local function response_ok(response: HttpResponse): boolean
	local status = response.status_code
	return response.success and status ~= nil and status >= 200 and status < 300
end

local function safe_json_decode(raw: string): JsonValue?
	local ok, value = pcall(json.decode, raw)
	if not ok then
		return nil
	end
	return value
end

-- Maps AudioMuse result rows to tracks, warning about ids that don't decode.
local function result_tracks(label: string, items: { JsonObject }): { MixResultTrack }
	local results: { MixResultTrack } = {}
	local decode_failed = 0
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
		local track_id = decode_track_id(result_item_id)
		if not track_id then
			decode_failed += 1
			continue
		end
		table.insert(results, { track_id = track_id })
	end

	-- Encoder/version mismatch; escalate above routine orphans.
	if decode_failed > 0 then
		warn(`{label}: {decode_failed}/{#items} id decode failed — possible ID format mismatch`)
	end

	return results
end

-- The JSON body of a successful response; `{}` for a 404 (seeds not yet analyzed), nil for a
-- backend failure.
local function response_body(label: string, response: HttpResponse): JsonValue?
	if not response_ok(response) then
		-- 404 = seed not yet analyzed; distinct from a backend outage.
		if response.success and response.status_code == 404 then
			return {}
		end
		warn(`{label} backend failure: status={response.status_code or 0} kind={response.error_kind or ""}`)
		return nil
	end

	local body = safe_json_decode(response.body)
	if type(body) ~= "table" then
		warn(`{label} returned non-JSON or non-object body`)
		return nil
	end
	return body
end

local function query_similar_tracks(item_id: string, n: number): { MixResultTrack }?
	local label = `similar_tracks({item_id})`
	local body = response_body(
		label,
		http.request({
			url = `{BASE_URL}/api/similar_tracks?item_id={item_id}&n={n}`,
			method = http.HttpMethod.Get,
			headers = auth_headers(),
		})
	)
	if body == nil then
		return nil
	end
	return result_tracks(label, body :: { JsonObject })
end

-- An item of an alchemy blend: a song by AudioMuse item id, added to or subtracted from the blend.
type AlchemyItem = { id: string, op: string, type: string }

local function query_alchemy(items: { AlchemyItem }, n: number): { MixResultTrack }?
	local label = `alchemy({#items} items)`
	local body = response_body(
		label,
		http.request({
			url = `{BASE_URL}/api/alchemy`,
			method = http.HttpMethod.Post,
			headers = json_post_headers(),
			body = json.encode({ items = items, n = n } :: any),
		})
	)
	if body == nil then
		return nil
	end
	local rows = (body :: JsonObject).results
	if rows == nil then
		return {}
	end
	if type(rows) ~= "table" then
		warn(`{label} returned non-array results`)
		return nil
	end
	return result_tracks(label, rows :: { JsonObject })
end

local function track_mix(seed_id: string, limit: number?): MixResult
	local item_id = encode_track_id(seed_id)
	if not item_id then
		warn(`track mix: seed {seed_id} could not be encoded; returning empty mix`)
		return { tracks = {} }
	end

	local n = (limit or 50) + 1
	local results = query_similar_tracks(item_id, n)
	if not results then
		return { tracks = {} }
	end

	-- Filter out the seed track from results
	local filtered: { MixResultTrack } = {}
	for _, result in results do
		if result.track_id ~= seed_id then
			table.insert(filtered, result)
		end
	end

	return { tracks = filtered }
end

-- The most tracks a release or artist seed adds to a blend. Alchemy treats every item alike,
-- so this bounds how much one release or artist outweighs a single track seed.
local SEED_ENTITY_TRACKS = 5
-- The most items one alchemy request carries.
local MAX_ALCHEMY_ITEMS = 100

-- The track ids a seed stands for: itself, or the first tracks of its release or artist.
-- Artists are expanded rather than sent as `type = "artist"`, since AudioMuse resolves artist
-- ids through a name map built from each track's album artist and first artist id, which a Lyra
-- artist id doesn't reliably reach.
local function seed_tracks(seed: mix.MixSeed): { string }
	if seed.type == mix.SeedType.Track then
		return { seed.id }
	end
	local seed_track_list: { tracks.Track } = if seed.type == mix.SeedType.Release
		then tracks.by_release({ seed.id })[seed.id] or {}
		else tracks.query({ artist_ids = { seed.id }, credit_role = "track" }).items
	local ids_out: { string } = {}
	for _, track in seed_track_list do
		if #ids_out >= SEED_ENTITY_TRACKS then
			break
		end
		table.insert(ids_out, track.id)
	end
	return ids_out
end

-- Blends the seeds through AudioMuse's alchemy: excluded seeds subtract, the rest add.
local function alchemy_mix(seeds: { mix.MixSeed }, limit: number?): MixResult
	local items: { AlchemyItem } = {}
	local seed_track_ids: { [string]: boolean } = {}
	local adds = 0
	for _, seed in seeds do
		local op = if seed.exclude then "SUBTRACT" else "ADD"
		for _, track_id in seed_tracks(seed) do
			if #items >= MAX_ALCHEMY_ITEMS then
				break
			end
			local item_id = if seed_track_ids[track_id] then nil else encode_track_id(track_id)
			if item_id then
				seed_track_ids[track_id] = true
				table.insert(items, { id = item_id, op = op, type = "song" })
				if op == "ADD" then
					adds += 1
				end
			end
		end
	end
	-- Alchemy needs something to add.
	if adds == 0 then
		return { tracks = {} }
	end

	local results = query_alchemy(items, limit or 50)
	if not results then
		return { tracks = {} }
	end
	local filtered: { MixResultTrack } = {}
	for _, result in results do
		if not seed_track_ids[result.track_id] then
			table.insert(filtered, result)
		end
	end
	return { tracks = filtered }
end

-- The most explicit seeds a blend takes; recent listens fill the rest. It matches the server's
-- `RECENT_LISTEN_COUNT`, so a history blend can draw on every recent listen, up to
-- `MAX_ALCHEMY_ITEMS` tracks.
local MAX_BLEND_SEEDS = 50

local mixer = mix.Mixer.new("audiomuse", { display_name = "AudioMuse AI" })

mixer:blend({
	seeds = { mix.SeedType.Track, mix.SeedType.Release, mix.SeedType.Artist },
	max_seeds = MAX_BLEND_SEEDS,
	exclude = true,
}, function(ctx: MixBlendContext): MixResult
	local seeds = ctx.seeds
	if #seeds == 1 and seeds[1].type == mix.SeedType.Track and not seeds[1].exclude then
		return track_mix(seeds[1].id, ctx.limit)
	end
	return alchemy_mix(seeds, ctx.limit)
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
		url = `{BASE_URL}/api/health`,
		method = http.HttpMethod.Get,
	})

	return api.response.json(response.status_code or 502, {
		healthy = response_ok(response),
		backend_url = BASE_URL,
	})
end)
