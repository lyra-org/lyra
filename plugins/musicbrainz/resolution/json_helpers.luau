--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json = require("@harmony/json")

type JsonValue = json.JsonValue
type JsonObject = { [string]: JsonValue }
type JsonArray = { JsonValue }

local module = {}

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

return module
