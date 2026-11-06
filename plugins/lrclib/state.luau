--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local datastore = require("@lyra/datastore")
local serde = require("@harmony/serde")

type JsonValue = serde.JsonValue
type DataStore = datastore.DataStore
type DataStoreRecord = datastore.DataStoreRecord

local STATE_STORE_NAME = "state"
local ATTEMPT_KEY_PREFIX = "attempt:"
local CACHE_KEY_PREFIX = "cache:v2:id:"
local CACHE_HIT_TTL_MS = 30 * 24 * 60 * 60 * 1000

local MISS_BACKOFF_BASE_MS = 60 * 60 * 1000
local MISS_BACKOFF_MAX_MS = 24 * 60 * 60 * 1000

export type AttemptStatus = "miss" | "instrumental" | "hit"

export type AttemptRecord = {
	status: AttemptStatus,
	last_attempt_at_ms: number,
	next_attempt_at_ms: number,
}

-- A track's lookup state, read in one batch; changes are held until `flush`
-- writes them in one transaction.
export type State = {
	prev_attempt: AttemptRecord?,
	cached_id: string?,
	set_attempt: (status: AttemptStatus, next_attempt_at_ms: number) -> (),
	set_cache: (lrclib_id: string) -> (),
	flush: () -> (),
}

local module = {}

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

function module.now_ms(): number
	return math.floor(os.time() * 1000)
end

local function decode_attempt(raw: JsonValue?): AttemptRecord?
	if type(raw) ~= "table" then
		return nil
	end
	local obj = raw :: { [string]: JsonValue }
	local status = obj.status
	if status ~= "miss" and status ~= "instrumental" and status ~= "hit" then
		return nil
	end
	local last_attempt_at_ms = obj.last_attempt_at_ms
	local next_attempt_at_ms = obj.next_attempt_at_ms
	return {
		status = status :: AttemptStatus,
		last_attempt_at_ms = if type(last_attempt_at_ms) == "number" then last_attempt_at_ms else 0,
		next_attempt_at_ms = if type(next_attempt_at_ms) == "number" then next_attempt_at_ms else 0,
	}
end

local function decode_cache(raw: JsonValue?): string?
	if type(raw) ~= "table" then
		return nil
	end
	local lrclib_id = (raw :: { [string]: JsonValue }).lrclib_id
	if type(lrclib_id) == "string" and lrclib_id ~= "" then
		return lrclib_id
	end
	return nil
end

-- The wait before retrying after a miss: doubling across consecutive misses,
-- from an hour up to a day.
function module.miss_backoff_ms(prev_record: AttemptRecord?): number
	if prev_record == nil or prev_record.status ~= "miss" then
		return MISS_BACKOFF_BASE_MS
	end
	local elapsed = math.max(prev_record.next_attempt_at_ms - prev_record.last_attempt_at_ms, 0)
	local doubled = math.max(elapsed * 2, MISS_BACKOFF_BASE_MS)
	return math.min(doubled, MISS_BACKOFF_MAX_MS)
end

-- Reads the state of `track_id`, whose LRCLIB lookup is described by `lookup`.
-- Attempt records stay until replaced; cached ids expire with the store.
function module.load(track_id: string, lookup: string): State
	local store = get_state_store()
	local attempt_key = `{ATTEMPT_KEY_PREFIX}{track_id}`
	local cache_key = `{CACHE_KEY_PREFIX}{lookup}`

	local raws: { JsonValue? } = if store ~= nil then store:get_many({ attempt_key, cache_key }) else { nil, nil }
	local pending: { [string]: DataStoreRecord } = {}

	local state: State = {
		prev_attempt = decode_attempt(raws[1]),
		cached_id = decode_cache(raws[2]),
		set_attempt = function(status: AttemptStatus, next_attempt_at_ms: number)
			pending[attempt_key] = {
				value = {
					status = status :: JsonValue,
					last_attempt_at_ms = module.now_ms() :: JsonValue,
					next_attempt_at_ms = next_attempt_at_ms :: JsonValue,
				} :: JsonValue,
			}
		end,
		set_cache = function(lrclib_id: string)
			pending[cache_key] = {
				value = { lrclib_id = lrclib_id :: JsonValue } :: JsonValue,
				ttl_ms = CACHE_HIT_TTL_MS,
			}
		end,
		flush = function()
			if store == nil or next(pending) == nil then
				return
			end
			pcall(store.set_many, store, pending)
		end,
	}
	return state
end

return module
