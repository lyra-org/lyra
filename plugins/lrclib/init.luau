--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local http = require("@harmony/http")
local json = require("@harmony/json")
local lyrics_api = require("@lyra/lyrics")
local metadata = require("@lyra/metadata")

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse
type DataStore = datastore.DataStore
type PluginLyricsInput = lyrics_api.PluginLyricsInput
type ProviderLyricsContext = metadata.ProviderLyricsContext

type LyricsCandidate = {
	lyrics: PluginLyricsInput,
	title: string,
	artist: string,
	duration_ms: number?,
	language: string,
}

type HandlerResult = {
	kind: string,
	candidates: { LyricsCandidate }?,
	retry_after_ms: number?,
}

-- Lyra requires `provider:lyrics` registration to declare a rate limit on at
-- least one domain. LRCLIB itself documents no server-side limits, so 4 req/s
-- is a self-imposed politeness floor; harmony_http handles transient 429s via
-- max_retries + Retry-After if they ever appear.
http.set_rate_limit({
	domain = "lrclib.net",
	requests_per_second = 4,
	max_retries = 1,
})

local API_BASE = "https://lrclib.net/api"
local STATE_STORE_NAME = "lrclib_state"
local ATTEMPT_KEY_PREFIX = "attempt:"
local CACHE_KEY_PREFIX = "cache:id:"
local CACHE_HIT_TTL_SEC = 30 * 24 * 60 * 60
local SEARCH_CANDIDATE_CAP = 5
local DEFAULT_LANGUAGE = "und"

local MISS_BACKOFF_BASE_MS = 60 * 60 * 1000
local MISS_BACKOFF_MAX_MS = 24 * 60 * 60 * 1000
local INSTRUMENTAL_BACKOFF_MS = 7 * 24 * 60 * 60 * 1000

local state_store: DataStore? = nil

local function get_state_store(): DataStore?
	if state_store ~= nil then
		return state_store
	end
	local ok, store = pcall(datastore.get_or_create, STATE_STORE_NAME)
	if not ok then
		return nil
	end
	state_store = store
	return store
end

local function now_ms(): number
	return math.floor(os.time() * 1000)
end

local function trim(value: string?): string
	if value == nil then
		return ""
	end
	local trimmed = string.gsub(value, "^%s+", "")
	trimmed = string.gsub(trimmed, "%s+$", "")
	return trimmed
end

local function as_json_object(value: JsonValue?): JsonObject?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "{" then
		return nil
	end
	return value :: JsonObject
end

local function as_json_array(value: JsonValue?): JsonArray?
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

local function json_number_field(value: JsonObject?, key: string): number?
	local field = if value then value[key] else nil
	if type(field) == "number" then
		return field
	end
	return nil
end

local function json_boolean_field(value: JsonObject?, key: string): boolean?
	local field = if value then value[key] else nil
	if type(field) == "boolean" then
		return field
	end
	return nil
end

local function json_id_field(value: JsonObject?, key: string): string?
	local field = if value then value[key] else nil
	if type(field) == "number" then
		return tostring(math.floor(field))
	end
	if type(field) == "string" and field ~= "" then
		return field
	end
	return nil
end

local function build_query(params: { [string]: string? }): string
	local parts: { string } = {}
	for key, value in params do
		if value ~= nil and trim(value) ~= "" then
			table.insert(parts, `{http.encode_uri_component(key)}={http.encode_uri_component(value)}`)
		end
	end
	if #parts == 0 then
		return ""
	end
	table.sort(parts)
	return `?{table.concat(parts, "&")}`
end

local function decode_json_value(body: string): JsonValue?
	local ok, decoded = pcall(json.decode, body)
	if not ok then
		return nil
	end
	return decoded
end

local function request_json(url: string): (HttpResponse, JsonValue?)
	local request: HttpRequestOptions = {
		url = url,
		method = http.HttpMethod.Get,
		headers = {
			Accept = "application/json",
		},
	}
	local response = http.request(request)
	if response.status_code ~= 200 then
		return response, nil
	end
	if type(response.body) ~= "string" or response.body == "" then
		return response, nil
	end
	return response, decode_json_value(response.body)
end

local function parse_retry_after_ms(response: HttpResponse): number?
	local headers = response.headers
	if type(headers) ~= "table" then
		return nil
	end
	local raw: string? = nil
	for key, value in headers do
		if string.lower(key) == "retry-after" and type(value) == "string" then
			raw = value
			break
		end
	end
	if raw == nil then
		return nil
	end
	local seconds = tonumber(trim(raw))
	if seconds == nil or seconds < 0 then
		return nil
	end
	return math.floor(seconds * 1000)
end

local function build_plain_only_input(id: string, plain_text: string): PluginLyricsInput
	return {
		id = id,
		language = DEFAULT_LANGUAGE,
		plain_text = plain_text,
		lines = {},
	}
end

local function candidate_from_response(response_object: JsonObject?): LyricsCandidate?
	if response_object == nil then
		return nil
	end

	local id = json_id_field(response_object, "id")
	if id == nil then
		return nil
	end

	local track_name = json_string_field(response_object, "trackName")
	local artist_name = json_string_field(response_object, "artistName")
	if track_name == nil or artist_name == nil then
		return nil
	end

	local duration_seconds = json_number_field(response_object, "duration")
	local duration_ms: number? = if duration_seconds ~= nil then math.floor(duration_seconds * 1000) else nil

	local synced = json_string_field(response_object, "syncedLyrics")
	local plain = json_string_field(response_object, "plainLyrics")

	local lyrics_input: PluginLyricsInput? = nil
	if synced ~= nil then
		local ok, parsed = pcall(lyrics_api.parse_lrc, synced, DEFAULT_LANGUAGE)
		if ok and type(parsed) == "table" then
			parsed.id = id
			lyrics_input = parsed
		end
	end
	if lyrics_input == nil and plain ~= nil then
		lyrics_input = build_plain_only_input(id, plain)
	end
	if lyrics_input == nil then
		return nil
	end

	return {
		lyrics = lyrics_input,
		title = track_name,
		artist = artist_name,
		duration_ms = duration_ms,
		language = DEFAULT_LANGUAGE,
	}
end

local function cache_key(ctx: ProviderLyricsContext): string
	local track = trim(ctx.track_name)
	local artist = trim(ctx.artist_name)
	local album = trim(ctx.album_name)
	local duration_rounded = if ctx.duration_ms ~= nil then tostring(math.floor(ctx.duration_ms / 1000)) else ""
	return `{CACHE_KEY_PREFIX}{track}|{artist}|{album}|{duration_rounded}`
end

local function attempt_key(ctx: ProviderLyricsContext): string
	return `{ATTEMPT_KEY_PREFIX}{ctx.track_db_id}`
end

type AttemptStatus = "miss" | "instrumental" | "hit"

type AttemptRecord = {
	status: AttemptStatus,
	last_attempt_at_ms: number,
	next_attempt_at_ms: number,
}

local function decode_attempt(raw: JsonValue?): AttemptRecord?
	local obj = as_json_object(raw)
	if obj == nil then
		return nil
	end
	local status = json_string_field(obj, "status")
	if status ~= "miss" and status ~= "instrumental" and status ~= "hit" then
		return nil
	end
	return {
		status = status :: AttemptStatus,
		last_attempt_at_ms = json_number_field(obj, "last_attempt_at_ms") or 0,
		next_attempt_at_ms = json_number_field(obj, "next_attempt_at_ms") or 0,
	}
end

local function encode_attempt(record: AttemptRecord): JsonObject
	return {
		status = record.status :: JsonValue,
		last_attempt_at_ms = record.last_attempt_at_ms :: JsonValue,
		next_attempt_at_ms = record.next_attempt_at_ms :: JsonValue,
	}
end

local function decode_cache(raw: JsonValue?): string?
	local entry = as_json_object(raw)
	if entry == nil then
		return nil
	end
	local expires_at = json_number_field(entry, "expires_at")
	if expires_at == nil or expires_at <= now_ms() then
		return nil
	end
	return json_string_field(entry, "lrclib_id")
end

local function encode_cache(lrclib_id: string): JsonObject
	return {
		lrclib_id = lrclib_id :: JsonValue,
		expires_at = (now_ms() + CACHE_HIT_TTL_SEC * 1000) :: JsonValue,
	}
end

local function miss_backoff_ms(prev_record: AttemptRecord?): number
	if prev_record == nil or prev_record.status ~= "miss" then
		return MISS_BACKOFF_BASE_MS
	end
	local elapsed = math.max(prev_record.next_attempt_at_ms - prev_record.last_attempt_at_ms, 0)
	local doubled = math.max(elapsed * 2, MISS_BACKOFF_BASE_MS)
	return math.min(doubled, MISS_BACKOFF_MAX_MS)
end

local function fetch_by_id(id: string): (HttpResponse, JsonObject?)
	local response, body = request_json(`{API_BASE}/get/{http.encode_uri_component(id)}`)
	return response, as_json_object(body)
end

local function fetch_search(ctx: ProviderLyricsContext): (HttpResponse, JsonArray?)
	local params: { [string]: string? } = {
		track_name = ctx.track_name,
		artist_name = ctx.artist_name,
	}
	local response, body = request_json(`{API_BASE}/search{build_query(params)}`)
	return response, as_json_array(body)
end

-- Hot-path contract: known-miss / known-instrumental decisions must resolve
-- without awaiting HTTP — datastore reads only.
--
-- DB lock budget: one batched read at entry, one batched write at exit.
-- All state mutations between them go into `pending` and flush together.
local function handle(ctx: ProviderLyricsContext): HandlerResult
	local store = get_state_store()
	local attempt_k = attempt_key(ctx)
	local cache_k = cache_key(ctx)

	local raws: { JsonValue? } = if store ~= nil then store:get_many({ attempt_k, cache_k }) else { nil, nil }

	local prev_attempt: AttemptRecord? = decode_attempt(raws[1])
	local cached_id: string? = decode_cache(raws[2])

	local pending: { [string]: JsonValue } = {}
	local function flush()
		if store == nil then
			return
		end
		if next(pending) == nil then
			return
		end
		pcall(store.set_many, store, pending)
	end

	local function set_attempt(status: AttemptStatus, next_attempt_at_ms: number)
		pending[attempt_k] = encode_attempt({
			status = status,
			last_attempt_at_ms = now_ms(),
			next_attempt_at_ms = next_attempt_at_ms,
		}) :: JsonValue
	end

	local function set_cache(lrclib_id: string)
		pending[cache_k] = encode_cache(lrclib_id) :: JsonValue
	end

	-- Negative-cache gate — skipped on force_refresh.
	if
		not ctx.force_refresh
		and prev_attempt ~= nil
		and (prev_attempt.status == "miss" or prev_attempt.status == "instrumental")
		and now_ms() < prev_attempt.next_attempt_at_ms
	then
		return { kind = prev_attempt.status }
	end

	local function resolve_response(body: JsonObject): HandlerResult
		if json_boolean_field(body, "instrumental") == true then
			set_attempt("instrumental", now_ms() + INSTRUMENTAL_BACKOFF_MS)
			flush()
			return { kind = "instrumental" }
		end
		local candidate = candidate_from_response(body)
		if candidate == nil then
			set_attempt("miss", now_ms() + miss_backoff_ms(prev_attempt))
			flush()
			return { kind = "miss" }
		end
		set_attempt("hit", math.huge)
		flush()
		return { kind = "hit", candidates = { candidate } }
	end

	-- Positive-cache fast path on the lrclib id.
	if cached_id ~= nil then
		local response, body = fetch_by_id(cached_id)
		if response.status_code == 429 then
			return { kind = "rate_limited", retry_after_ms = parse_retry_after_ms(response) }
		end
		if response.status_code == 200 and body ~= nil then
			return resolve_response(body)
		end
	end

	local search_response, search_body = fetch_search(ctx)
	if search_response.status_code == 429 then
		return { kind = "rate_limited", retry_after_ms = parse_retry_after_ms(search_response) }
	end
	if search_response.status_code ~= 200 then
		error(`lrclib /search failed with status {search_response.status_code}`)
	end
	if search_body == nil or #search_body == 0 then
		set_attempt("miss", now_ms() + miss_backoff_ms(prev_attempt))
		flush()
		return { kind = "miss" }
	end

	local candidates: { LyricsCandidate } = {}
	for _, entry in search_body do
		if #candidates >= SEARCH_CANDIDATE_CAP then
			break
		end
		local entry_object = as_json_object(entry)
		if entry_object ~= nil and json_boolean_field(entry_object, "instrumental") ~= true then
			local candidate = candidate_from_response(entry_object)
			if candidate ~= nil then
				table.insert(candidates, candidate)
			end
		end
	end
	if #candidates == 0 then
		set_attempt("miss", now_ms() + miss_backoff_ms(prev_attempt))
		flush()
		return { kind = "miss" }
	end
	set_cache(candidates[1].lyrics.id)
	set_attempt("hit", math.huge)
	flush()
	return { kind = "hit", candidates = candidates }
end

local provider = metadata.Provider.new("lrclib")

provider:lyrics({
	require = { all_of = { "track_name", "artist_name" } },
	priority = 50,
	timeout_ms = 30000,
}, function(ctx: ProviderLyricsContext): JsonValue
	return handle(ctx) :: JsonValue
end)
