--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Requests to the AudioMuse API. Every request names the configured AudioMuse server, if any:
-- GETs in the query string, POSTs in the JSON body.

local http = require("@harmony/http")
local serde = require("@harmony/serde")
local json = serde.json

type JsonValue = serde.JsonValue
type JsonObject = { [string]: JsonValue }
type HttpRequestOptions = http.HttpRequestOptions
type HttpResponse = http.HttpResponse

export type Config = {
	base_url: string,
	token: string,
	-- The AudioMuse media server name or id; empty for AudioMuse's default server.
	server: string,
}

local client = {}

-- Sends a request; tests replace it to answer with scripted responses.
client.send = http.request :: (HttpRequestOptions) -> HttpResponse

local config: Config = { base_url = "", token = "", server = "" }

function client.configure(next_config: Config)
	config = table.clone(next_config)
end

function client.base_url(): string
	return config.base_url
end

local function auth_headers(): { [string]: string }
	if config.token ~= "" then
		return { Authorization = `Bearer {config.token}` }
	end
	return {}
end

-- The query string for `params` plus the configured server; values are already strings.
local function query_string(params: { [string]: string }): string
	local parts: { string } = {}
	if config.server ~= "" then
		params = table.clone(params)
		params.server = config.server
	end
	local names: { string } = {}
	for name in params do
		table.insert(names, name)
	end
	table.sort(names)
	for _, name in names do
		table.insert(parts, `{name}={http.encode_uri_component(params[name])}`)
	end
	if #parts == 0 then
		return ""
	end
	return "?" .. table.concat(parts, "&")
end

function client.get(path: string, params: { [string]: string }?): HttpResponse
	return client.send({
		url = `{config.base_url}{path}{query_string(params or {})}`,
		method = http.HttpMethod.Get,
		headers = auth_headers(),
	})
end

-- Posts `body` as JSON; AudioMuse's Flask endpoints require `Content-Type: application/json`.
function client.post(path: string, body: JsonObject): HttpResponse
	if config.server ~= "" then
		body = table.clone(body)
		body.server = config.server
	end
	local headers = auth_headers()
	headers["Content-Type"] = "application/json"
	return client.send({
		url = `{config.base_url}{path}`,
		method = http.HttpMethod.Post,
		headers = headers,
		-- An empty table encodes as an array.
		body = if next(body) == nil then "{}" else json.encode(body :: any),
	})
end

-- Whether AudioMuse answered with a 2xx status; `success` alone only means a response arrived.
function client.ok(response: HttpResponse): boolean
	local status = response.status_code
	return response.success and status ~= nil and status >= 200 and status < 300
end

function client.decode(raw: string): JsonValue?
	local ok, value = pcall(json.decode, raw)
	if not ok then
		return nil
	end
	return value
end

-- The `error` message of an AudioMuse error body, else its status.
function client.error_message(response: HttpResponse): string
	local body = client.decode(response.body)
	if type(body) == "table" then
		local message = (body :: JsonObject).error_message or (body :: JsonObject).error
		if type(message) == "string" then
			return message
		end
	end
	if response.status_code then
		return `status {response.status_code}`
	end
	return response.error_kind or "request failed"
end

return client
