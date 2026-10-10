--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local server = require("@lyra/server")

local serde = require("@harmony/serde")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

export type SubsonicError = {
	code: number,
	message: string,
	helpUrl: string?,
}

local PROTOCOL_VERSION = "1.16.1"
local SERVER_TYPE = "lyra"
local SERVER_VERSION = server.info().version
local XML_NAMESPACE = "http://subsonic.org/restapi"
local JSONP_CALLBACK_PATTERN = "^[%a_$][%w_$.]*$"

local module = {}

-- Returns a query parameter's first non-empty value.
function module.param(query: ApiQueryParams, name: string): string?
	local values = query[name]
	local value = if values then values[1] else nil
	return if value ~= nil and value ~= "" then value else nil
end

function module.error(code: number, message: string, help_url: string?): SubsonicError
	return {
		code = code,
		message = message,
		helpUrl = help_url,
	}
end

local function envelope(status: string, body: JsonObject?): JsonObject
	local fields: JsonObject = {
		status = status,
		version = PROTOCOL_VERSION,
		type = SERVER_TYPE,
		serverVersion = SERVER_VERSION,
		openSubsonic = true,
	}
	for key, value in body or {} do
		fields[key] = value
	end
	return fields
end

-- Subsonic XML carries scalars as attributes and lists as repeated elements.
local function to_xml_element(value: { [string]: any }): { [string]: any }
	local element = {}
	for key, child in value do
		if type(child) ~= "table" then
			element[`@{key}`] = child
		elseif child[1] ~= nil then
			local items = {}
			for _, item in child do
				table.insert(items, if type(item) == "table" then to_xml_element(item) else item)
			end
			element[key] = items
		elseif next(child) ~= nil then
			element[key] = to_xml_element(child)
		end
	end
	return element
end

local function render(query: ApiQueryParams, fields: JsonObject, http_status: number): ApiResponse
	local format = module.param(query, "f")
	if format == "json" or format == "jsonp" then
		local json = serde.json.encode({ ["subsonic-response"] = fields })
		if format == "json" then
			return api.response.text(http_status, json, { ["content-type"] = "application/json; charset=utf-8" })
		end
		local callback = module.param(query, "callback")
		if not callback or not string.match(callback, JSONP_CALLBACK_PATTERN) then
			local missing = envelope("failed", { error = module.error(10, "Required parameter is missing: callback") })
			return render({ f = { "json" } }, missing, 200)
		end
		return api.response.text(
			http_status,
			`{callback}({json});`,
			{ ["content-type"] = "application/javascript; charset=utf-8" }
		)
	end

	local element = to_xml_element(fields)
	element["@xmlns"] = XML_NAMESPACE
	local xml = serde.xml.encode("subsonic-response", element :: any)
	return api.response.text(
		http_status,
		`<?xml version="1.0" encoding="UTF-8"?>{xml}`,
		{ ["content-type"] = "application/xml; charset=utf-8" }
	)
end

function module.ok(query: ApiQueryParams, body: JsonObject?): ApiResponse
	return render(query, envelope("ok", body), 200)
end

function module.failed(query: ApiQueryParams, err: SubsonicError, http_status: number?): ApiResponse
	return render(query, envelope("failed", { error = err :: any }), http_status or 200)
end

return module
