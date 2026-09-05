--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json

export type JsonValue = serde.JsonValue
export type JsonObject = { [string]: JsonValue }
export type JsonArray = { JsonValue }

local WIKIDATA_API = "https://www.wikidata.org/w/api.php"
local CACHE_STORE_NAME = "wikidata"

local module = {}

module.HIT_CACHE_TTL = 30 * 24 * 60 * 60
module.MISS_CACHE_TTL = 7 * 24 * 60 * 60 -- allows recovery
module.SHORT_MISS_CACHE_TTL = 6 * 60 * 60 -- ambiguous or partial results

-- Wikimedia allows ~1000 req/hr for unauthenticated clients with a compliant
-- User-Agent (set server-wide via outbound_user_agent). 0.2 req/s = 720 req/hr,
-- well within the hourly quota.
module.RATE_LIMIT = {
	requests_per_second = 0.2,
	retry_on = { 429, 503 },
	max_retries = 3,
	backoff_ms = 5000,
}

http.set_rate_limit({
	domain = "www.wikidata.org",
	requests_per_second = module.RATE_LIMIT.requests_per_second,
	retry_on = module.RATE_LIMIT.retry_on,
	max_retries = module.RATE_LIMIT.max_retries,
	backoff_ms = module.RATE_LIMIT.backoff_ms,
})

function module.as_json_object(value: JsonValue): JsonObject?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "{" then
		return nil
	end
	return value :: JsonObject
end

function module.as_json_array(value: JsonValue): JsonArray?
	if type(value) ~= "table" then
		return nil
	end
	local encoded = json.encode(value)
	if string.sub(encoded, 1, 1) ~= "[" then
		return nil
	end
	return value :: JsonArray
end

function module.json_string_field(value: JsonObject?, key: string): string?
	local field = if value then value[key] else nil
	if type(field) == "string" and field ~= "" then
		return field
	end
	return nil
end

function module.is_valid_qid(value: string?): boolean
	if not value then
		return false
	end
	return string.match(value, "^Q%d+$") ~= nil
end

function module.trim(value: string): string
	return (string.gsub(value, "^%s+", ""):gsub("%s+$", ""))
end

local cache_store: datastore.DataStore? = nil

local function get_cache_store(): datastore.DataStore?
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

function module.cache_get(key: string): JsonObject?
	local store = get_cache_store()
	if not store then
		return nil
	end
	local ok, raw = pcall(store.get, store, key)
	if not ok then
		return nil
	end
	local entry = module.as_json_object(raw)
	if not entry then
		return nil
	end
	local expires_at = entry.expires_at
	if type(expires_at) ~= "number" or expires_at <= os.time() then
		return nil
	end
	return module.as_json_object(entry.data)
end

function module.cache_set(key: string, data: JsonValue, ttl: number)
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

-- Returns the decoded response object, or nil when the request failed at the
-- transport level, returned a non-200 status, was not JSON, or carried a
-- MediaWiki error envelope. Callers must not cache a miss on nil.
function module.api_get(url: string): JsonObject?
	local request: http.HttpRequestOptions = {
		url = url,
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

	local root = module.as_json_object(data)
	if not root or root.error ~= nil then
		return nil
	end
	return root
end

function module.wikidata_get(params: string): JsonObject?
	return module.api_get(`{WIKIDATA_API}?{params}&format=json`)
end

local function claim_datavalue(claim: JsonValue): JsonValue?
	local claim_obj = module.as_json_object(claim)
	if not claim_obj then
		return nil
	end
	local mainsnak = module.as_json_object(claim_obj.mainsnak)
	if not mainsnak then
		return nil
	end
	local datavalue = module.as_json_object(mainsnak.datavalue)
	if not datavalue then
		return nil
	end
	return datavalue.value
end

function module.extract_claim_qid(claim: JsonValue): string?
	local value = module.as_json_object(claim_datavalue(claim))
	if not value then
		return nil
	end
	local qid = module.json_string_field(value, "id")
	if not module.is_valid_qid(qid) then
		return nil
	end
	return qid
end

function module.extract_claim_string(claim: JsonValue): string?
	local value = claim_datavalue(claim)
	if type(value) == "string" and value ~= "" then
		return value
	end
	return nil
end

return module
