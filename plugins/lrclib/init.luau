--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json
local lyrics_api = require("@lyra/lyrics")
local metadata = require("@lyra/metadata")
local state = require("@self/state")

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse
type ProviderLyricsInput = metadata.ProviderLyricsInput
type ProviderLyricsCandidate = metadata.ProviderLyricsCandidate
type ProviderLyricsContext = metadata.ProviderLyricsContext
type ProviderLyricsResult = metadata.ProviderLyricsResult

type LyricsCandidate = ProviderLyricsCandidate

type HandlerResult = ProviderLyricsResult

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
local SEARCH_CANDIDATE_CAP = 5
local DEFAULT_LANGUAGE = "und"

local INSTRUMENTAL_BACKOFF_MS = 7 * 24 * 60 * 60 * 1000

local now_ms = state.now_ms

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

local function build_plain_only_input(id: string, plain_text: string): ProviderLyricsInput
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

	local lyrics_input: ProviderLyricsInput? = nil
	if synced ~= nil then
		local ok, parsed = pcall(lyrics_api.parse_lrc, synced, { language = DEFAULT_LANGUAGE })
		if ok and type(parsed) == "table" then
			lyrics_input = {
				id = id,
				language = parsed.language,
				plain_text = parsed.plain_text,
				lines = parsed.lines,
			}
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

local function lookup_key(ctx: ProviderLyricsContext): string
	local track = trim(ctx.track_name)
	local artist = trim(ctx.artist_name)
	local album = trim(ctx.album_name)
	local duration_rounded = if ctx.duration_ms ~= nil then tostring(math.floor(ctx.duration_ms / 1000)) else ""
	return `{track}|{artist}|{album}|{duration_rounded}`
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
-- DB lock budget: one batched read at entry, one batched write (a single
-- transaction) at exit: state changes are held until `flush`.
local function handle(ctx: ProviderLyricsContext): HandlerResult
	local lookup_state = state.load(ctx.track_id, lookup_key(ctx))
	local prev_attempt = lookup_state.prev_attempt
	local cached_id = lookup_state.cached_id
	local set_attempt = lookup_state.set_attempt
	local set_cache = lookup_state.set_cache
	local flush = lookup_state.flush

	-- Negative-cache gate — skipped on force_refresh.
	if
		not ctx.force_refresh
		and prev_attempt ~= nil
		and (prev_attempt.status == "miss" or prev_attempt.status == "instrumental")
		and now_ms() < prev_attempt.next_attempt_at_ms
	then
		if prev_attempt.status == "instrumental" then
			return { kind = "instrumental" }
		end
		return { kind = "miss" }
	end

	local function resolve_response(body: JsonObject): HandlerResult
		if json_boolean_field(body, "instrumental") == true then
			set_attempt("instrumental", now_ms() + INSTRUMENTAL_BACKOFF_MS)
			flush()
			return { kind = "instrumental" }
		end
		local candidate = candidate_from_response(body)
		if candidate == nil then
			set_attempt("miss", now_ms() + state.miss_backoff_ms(prev_attempt))
			flush()
			return { kind = "miss" }
		end
		set_attempt("hit", now_ms())
		flush()
		return { kind = "hit", candidates = { candidate } }
	end

	-- Positive-cache fast path on the lrclib id. Force refresh must re-run
	-- search because the cached id may be a candidate Lyra's scorer rejected.
	if cached_id ~= nil and not ctx.force_refresh then
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
		set_attempt("miss", now_ms() + state.miss_backoff_ms(prev_attempt))
		flush()
		return { kind = "miss" }
	end

	local candidates: { LyricsCandidate } = {}
	local parsed_entries = 0
	local instrumental_entries = 0
	for _, entry in search_body do
		local entry_object = as_json_object(entry)
		if entry_object == nil then
			continue
		end
		parsed_entries += 1
		if json_boolean_field(entry_object, "instrumental") == true then
			instrumental_entries += 1
		elseif #candidates < SEARCH_CANDIDATE_CAP then
			local candidate = candidate_from_response(entry_object)
			if candidate ~= nil then
				table.insert(candidates, candidate)
			end
		end
	end
	if #candidates == 0 then
		-- Every parseable entry was explicitly instrumental — use the longer
		-- backoff so we don't re-poll LRCLIB hourly for a known instrumental.
		if parsed_entries > 0 and instrumental_entries == parsed_entries then
			set_attempt("instrumental", now_ms() + INSTRUMENTAL_BACKOFF_MS)
			flush()
			return { kind = "instrumental" }
		end
		set_attempt("miss", now_ms() + state.miss_backoff_ms(prev_attempt))
		flush()
		return { kind = "miss" }
	end
	set_cache(candidates[1].lyrics.id)
	set_attempt("hit", now_ms())
	flush()
	return { kind = "hit", candidates = candidates }
end

local provider = metadata.Provider.new("lrclib", { display_name = "LRCLIB" })

provider:lyrics({
	require = { all_of = { "track_name", "artist_name" } },
	timeout_ms = 30000,
}, function(ctx: ProviderLyricsContext): ProviderLyricsResult
	return handle(ctx)
end)
