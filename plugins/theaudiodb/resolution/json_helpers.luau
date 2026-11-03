--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local serde = require("@harmony/serde")
local json = serde.json

export type JsonValue = serde.JsonValue
export type JsonObject = { [string]: JsonValue }
export type JsonArray = { JsonValue }

local module = {}

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
	if type(field) == "string" then
		local trimmed = trim(field)
		if trimmed ~= "" then
			return trimmed
		end
	end
	return nil
end

local function json_number_field(value: JsonObject?, key: string): number?
	local field = if value then value[key] else nil
	if type(field) == "number" then
		return field
	end
	if type(field) == "string" then
		local parsed = tonumber(trim(field))
		if parsed ~= nil then
			return parsed
		end
	end
	return nil
end

local function first_non_empty_field(value: JsonObject?, keys: { string }): string?
	for _, key in keys do
		local field = json_string_field(value, key)
		if field ~= nil then
			return field
		end
	end
	return nil
end

module.trim = trim
module.as_json_object = as_json_object
module.as_json_array = as_json_array
module.json_string_field = json_string_field
module.json_number_field = json_number_field
module.first_non_empty_field = first_non_empty_field

return module
