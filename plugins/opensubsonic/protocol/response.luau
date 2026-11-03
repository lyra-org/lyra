--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local server = require("@lyra/server")

local serde = require("@harmony/serde")

local request = require("./request")

type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

export type SubsonicError = request.SubsonicError

local PROTOCOL_VERSION = "1.16.1"
local SERVER_TYPE = "lyra"
local SERVER_VERSION = server.info().version
local XML_NAMESPACE = "http://subsonic.org/restapi"
local JSONP_CALLBACK_PATTERN = "^[%a_$][%w_$.]*$"

local module = {}

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

-- Marks an empty object, such as a result with nothing found. Any other empty table is an empty
-- list: `[]` in JSON, and no elements in XML.
module.EMPTY_OBJECT = serde.empty_object()

-- Elements whose field of this name is the element's text rather than an attribute.
local XML_TEXT_FIELDS: { [string]: string } = {
	genre = "value",
	line = "value",
}

-- Subsonic XML carries scalars as attributes and lists as repeated elements.
local function to_xml_element(value: { [string]: any }, name: string?): { [string]: any }
	local text_field = if name then XML_TEXT_FIELDS[name] else nil
	local element = {}
	for key, child in value do
		if key == text_field then
			element["$text"] = child
		elseif child == module.EMPTY_OBJECT then
			element[key] = module.EMPTY_OBJECT
		elseif type(child) ~= "table" then
			element[`@{key}`] = child
		elseif child[1] ~= nil then
			local items = {}
			for _, item in child do
				table.insert(items, if type(item) == "table" then to_xml_element(item, key) else item)
			end
			element[key] = items
		elseif next(child) ~= nil then
			local nested = to_xml_element(child, key)
			element[key] = if next(nested) ~= nil then nested else module.EMPTY_OBJECT
		end
	end
	return element
end

-- How a response is written: as JSON, wrapped in a JSONP call when `callback` is set, or as XML.
type Format = { json: boolean, callback: string?, content_type: string }

-- The format a query asks for; nil for a JSONP call without a valid callback.
local function format_of(query: ApiQueryParams): Format?
	local format = request.param(query, "f")
	if format == "json" then
		return { json = true, content_type = "application/json; charset=utf-8" }
	elseif format == "jsonp" then
		local callback = request.param(query, "callback")
		if not callback or not string.match(callback, JSONP_CALLBACK_PATTERN) then
			return nil
		end
		return { json = true, callback = callback, content_type = "application/javascript; charset=utf-8" }
	end
	return { json = false, content_type = "text/xml; charset=utf-8" }
end

local XML_DECLARATION = `<?xml version="1.0" encoding="UTF-8"?>`

local function render(query: ApiQueryParams, fields: JsonObject, http_status: number): ApiResponse
	local format = format_of(query)
	if not format then
		-- A JSONP call without a valid callback fails in JSON.
		local missing = envelope("failed", { error = module.error(10, "Required parameter is missing: callback") })
		return render({ f = { "json" } }, missing, 200)
	end
	local headers = { ["content-type"] = format.content_type }
	if format.json then
		local json = serde.json.encode({ ["subsonic-response"] = fields })
		return api.response.text(http_status, if format.callback then `{format.callback}({json});` else json, headers)
	end

	local element = to_xml_element(fields)
	element["@xmlns"] = XML_NAMESPACE
	local xml = serde.xml.encode("subsonic-response", element :: any)
	return api.response.text(http_status, `{XML_DECLARATION}{xml}`, headers)
end

function module.ok(query: ApiQueryParams, body: JsonObject?): ApiResponse
	return render(query, envelope("ok", body), 200)
end

-- A list of a streamed response: its element name, and what hands its items over a batch at a
-- time.
export type StreamedList = {
	name: string,
	each_batch: (emit: ({ JsonObject }) -> ()) -> (),
}

-- An `ok` response whose `container` holds `lists`, in order, each sent as its batches are written,
-- so a long list never exists whole. The document is as `ok` would answer with the same lists; a
-- failure while producing aborts it.
function module.ok_stream(query: ApiQueryParams, container: string, lists: { StreamedList }): ApiResponse
	local format = format_of(query)
	if not format then
		-- Fails as `ok` does.
		return module.ok(query)
	end
	local names: { [string]: boolean } = {}
	for _, list in lists do
		assert(not names[list.name], `list {list.name} comes twice`)
		names[list.name] = true
	end
	local fields = envelope("ok")
	local headers = { ["content-type"] = format.content_type }

	if format.json then
		local callback = format.callback
		-- In key order, as a whole document encodes.
		local keys: { string } = {}
		for key in fields do
			table.insert(keys, key)
		end
		table.sort(keys)
		return api.response.stream(200, function(writer)
			local encoder = serde.json.encoder()
			local function write_fields(before: boolean)
				for _, key in keys do
					if (key < container) == before then
						encoder:field(key, fields[key])
					end
				end
			end
			if callback then
				writer:write(`{callback}(`)
			end
			encoder:begin_object()
			encoder:begin_object("subsonic-response")
			write_fields(true)
			encoder:begin_object(container)
			for _, list in lists do
				local open = false
				list.each_batch(function(items)
					if #items == 0 then
						return
					end
					if not open then
						encoder:begin_array(list.name)
						open = true
					end
					encoder:items(items :: any)
					items = nil :: any
					writer:write(encoder:flush())
				end)
				if open then
					encoder:end_array()
				end
			end
			encoder:end_object()
			write_fields(false)
			encoder:end_object()
			encoder:end_object()
			local rest = encoder:finish()
			writer:write(if callback then `{rest});` else rest)
		end, headers)
	end

	local attributes: JsonObject = table.clone(fields)
	attributes.xmlns = XML_NAMESPACE
	return api.response.stream(200, function(writer)
		local encoder = serde.xml.encoder()
		writer:write(XML_DECLARATION)
		encoder:begin_element("subsonic-response", attributes :: any)
		encoder:begin_element(container)
		for _, list in lists do
			list.each_batch(function(items)
				local elements = {}
				for index, item in items do
					elements[index] = to_xml_element(item, list.name)
				end
				items = nil :: any
				encoder:elements(list.name, elements :: any)
				elements = nil :: any
				writer:write(encoder:flush())
			end)
		end
		encoder:end_element()
		encoder:end_element()
		writer:write(encoder:finish())
	end, headers)
end

function module.failed(query: ApiQueryParams, err: SubsonicError, http_status: number?): ApiResponse
	return render(query, envelope("failed", { error = err :: any }), http_status or 200)
end

-- A failure in XML, whatever format the request asks for.
function module.failed_xml(err: SubsonicError): ApiResponse
	return render({}, envelope("failed", { error = err :: any }), 200)
end

return module
