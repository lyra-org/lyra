--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local datastore = require("@lyra/datastore")

local base = require("./items/common")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }
type DataStore = datastore.DataStore
type StoredValue = datastore.JsonValue
type StoredObject = { [string]: StoredValue }

local DISPLAY_PREFS_STORE_NAME = "jellyfin:display_prefs"
local DEVICE_OPTIONS_STORE_NAME = "jellyfin:device_options"
local MAX_BLOB_BYTES = 64 * 1024
local MAX_CLIENT_SEGMENT_LENGTH = 128
local MAX_PREFS_ID_LENGTH = 128
local MAX_DEVICE_ID_LENGTH = 128

-- `:` is safe alongside the packed-key separator: length prefixes keep keys injective.
local SAFE_SEGMENT_PATTERN = "^[%w_%-%.:]+$"

local display_prefs_store: DataStore? = nil
local device_options_store: DataStore? = nil

local module = {}

local function resolve_store(cached: DataStore?, name: string): (DataStore?, DataStore?)
	if cached ~= nil then
		return cached, cached
	end
	local ok, store = pcall(datastore.get_or_create, name)
	if not ok or not store then
		return nil, nil
	end
	return store, store
end

local function get_display_prefs_store(): DataStore?
	local store, cache_value = resolve_store(display_prefs_store, DISPLAY_PREFS_STORE_NAME)
	display_prefs_store = cache_value
	return store
end

local function get_device_options_store(): DataStore?
	local store, cache_value = resolve_store(device_options_store, DEVICE_OPTIONS_STORE_NAME)
	device_options_store = cache_value
	return store
end

local function valid_segment(value: string?, max_length: number): string?
	if type(value) ~= "string" or value == "" then
		return nil
	end
	if #value > max_length then
		return nil
	end
	if not string.match(value, SAFE_SEGMENT_PATTERN) then
		return nil
	end
	return value
end

local function require_client(ctx: ApiContext): string?
	local query = if ctx and ctx.request then ctx.request.query else nil
	local raw = base.parse_query_string(query, { "client", "Client" }, nil)
	return valid_segment(raw, MAX_CLIENT_SEGMENT_LENGTH)
end

local function require_device_id(ctx: ApiContext): string?
	local query = if ctx and ctx.request then ctx.request.query else nil
	local raw = base.parse_query_string(query, { "id", "Id" }, nil)
	return valid_segment(raw, MAX_DEVICE_ID_LENGTH)
end

local function display_prefs_key(user_id: number, client: string, prefs_id: string): string
	-- Length-prefix each variable segment so no separator injection is possible.
	return string.format("%d:%d:%s:%d:%s", user_id, #client, client, #prefs_id, prefs_id)
end

local function device_options_key(user_id: number, device_id: string): string
	return string.format("%d:%d:%s", user_id, #device_id, device_id)
end

local function default_display_prefs(prefs_id: string, client: string): JsonObject
	return {
		Id = prefs_id,
		Client = client,
		SortBy = "SortName",
		SortOrder = "Ascending",
		RememberIndexing = false,
		PrimaryImageHeight = 0,
		PrimaryImageWidth = 0,
		ScrollDirection = "Horizontal",
		ShowBackdrop = true,
		RememberSorting = false,
		CustomPrefs = {},
	}
end

local function default_device_options(device_id: string): JsonObject
	return {
		Id = device_id,
		DeviceId = device_id,
	}
end

local function body_within_size_cap(ctx: ApiContext): boolean
	local request = if ctx then ctx.request else nil
	local raw = if request then request.body_raw else nil
	if type(raw) ~= "string" then
		return false
	end
	return #raw <= MAX_BLOB_BYTES
end

function module.display_prefs_get(ctx: ApiContext, user_id: number, prefs_id_raw: string): ApiResponse
	local prefs_id = valid_segment(prefs_id_raw, MAX_PREFS_ID_LENGTH)
	if not prefs_id then
		return base.bad_request("invalid displayPreferencesId")
	end
	local client = require_client(ctx)
	if not client then
		return base.bad_request("invalid or missing client")
	end

	local store = get_display_prefs_store()
	if not store then
		return api.response.empty(500)
	end

	local key = display_prefs_key(user_id, client, prefs_id)
	local get_ok, existing = pcall(store.get, store, key)
	if not get_ok then
		return api.response.empty(500)
	end

	local merged = default_display_prefs(prefs_id, client)
	if type(existing) == "table" then
		for field, value in existing :: StoredObject do
			merged[field] = value :: JsonValue
		end
	end
	merged.Id = prefs_id
	merged.Client = client
	return api.response.json(200, merged)
end

function module.display_prefs_set(ctx: ApiContext, user_id: number, prefs_id_raw: string): ApiResponse
	local prefs_id = valid_segment(prefs_id_raw, MAX_PREFS_ID_LENGTH)
	if not prefs_id then
		return base.bad_request("invalid displayPreferencesId")
	end
	local client = require_client(ctx)
	if not client then
		return base.bad_request("invalid or missing client")
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return base.bad_request("invalid display preferences body")
	end
	if not body_within_size_cap(ctx) then
		return base.bad_request("display preferences body exceeds size cap")
	end

	local store = get_display_prefs_store()
	if not store then
		return api.response.empty(500)
	end

	local key = display_prefs_key(user_id, client, prefs_id)
	local set_ok = pcall(store.set, store, key, request_json :: StoredValue)
	if not set_ok then
		return api.response.empty(500)
	end
	return api.response.empty(204)
end

function module.device_options_get(ctx: ApiContext, user_id: number): ApiResponse
	local device_id = require_device_id(ctx)
	if not device_id then
		return base.bad_request("invalid or missing id")
	end

	local store = get_device_options_store()
	if not store then
		return api.response.empty(500)
	end

	local key = device_options_key(user_id, device_id)
	local get_ok, existing = pcall(store.get, store, key)
	if not get_ok then
		return api.response.empty(500)
	end

	local merged = default_device_options(device_id)
	if type(existing) == "table" then
		for field, value in existing :: StoredObject do
			merged[field] = value :: JsonValue
		end
	end
	merged.Id = device_id
	merged.DeviceId = device_id
	return api.response.json(200, merged)
end

function module.device_options_set(ctx: ApiContext, user_id: number): ApiResponse
	local device_id = require_device_id(ctx)
	if not device_id then
		return base.bad_request("invalid or missing id")
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return base.bad_request("invalid device options body")
	end
	if not body_within_size_cap(ctx) then
		return base.bad_request("device options body exceeds size cap")
	end

	local payload = request_json :: JsonObject
	payload.Id = device_id
	payload.DeviceId = device_id

	local store = get_device_options_store()
	if not store then
		return api.response.empty(500)
	end

	local key = device_options_key(user_id, device_id)
	local set_ok = pcall(store.set, store, key, payload :: StoredValue)
	if not set_ok then
		return api.response.empty(500)
	end
	return api.response.empty(204)
end

return module
