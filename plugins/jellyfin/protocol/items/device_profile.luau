--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- A client's device profile, read as upstream's `ContainerHelper` and `StreamBuilder` read it.

local api = require("@lyra/api")

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

local module = {}

-- Upstream reads JSON numbers into string properties as their text.
function module.string_field(object: JsonObject?, key: string): string?
	local value = if object then object[key] else nil
	if type(value) == "string" then
		return value
	elseif type(value) == "number" then
		return tostring(value)
	end
	return nil
end

function module.object_list(object: JsonObject?, key: string): { JsonObject }
	local value = if object then object[key] else nil
	local list: { JsonObject } = {}
	if type(value) == "table" then
		for _, item in value :: { JsonValue } do
			if type(item) == "table" then
				table.insert(list, item :: JsonObject)
			end
		end
	end
	return list
end

-- An integer as .NET's `int.TryParse` reads one: surrounding whitespace and a sign allowed.
function module.parse_int(value: string?): number?
	if value == nil then
		return nil
	end
	local digits = string.match(value, "^%s*([+-]?%d+)%s*$")
	local number = if digits then tonumber(digits) else nil
	if number == nil or number < -2147483648 or number > 2147483647 then
		return nil
	end
	return number
end

-- An enum as upstream's deserializer reads one: by name in any case, or by number. A missing
-- value is the enum's first.
local function enum_value(raw: JsonValue, names: { [number]: string }): string?
	if raw == nil then
		return names[0]
	end
	if type(raw) == "string" then
		local lowered = string.lower(raw)
		for _, name in names do
			if string.lower(name) == lowered then
				return name
			end
		end
	end
	local number = tonumber(raw)
	return if number then names[number] else nil
end

local PROFILE_TYPES: { [number]: string } = { [0] = "Audio", "Video", "Photo", "Subtitle", "Lyric" }
local ENCODING_CONTEXTS: { [number]: string } = { [0] = "Streaming", "Static" }

function module.is_audio_profile(profile: JsonObject): boolean
	return enum_value(profile.Type, PROFILE_TYPES) == "Audio"
end

function module.is_streaming_profile(profile: JsonObject): boolean
	return enum_value(profile.Context, ENCODING_CONTEXTS) == "Streaming"
end

-- Whether a comma-separated profile list names any of the input's comma-separated values. An
-- empty profile list names everything; a negative one names what it doesn't list.
local function list_names(profile_list: string?, negative: boolean, input: string): boolean
	if profile_list == nil or profile_list == "" then
		return true
	end
	for input_value in string.gmatch(input, "[^,]+") do
		local lowered = string.lower(input_value)
		for profile_value in string.gmatch(profile_list, "[^,]+") do
			if lowered == string.lower(profile_value) then
				return not negative
			end
		end
	end
	return negative
end

-- Upstream's `ContainerHelper.ContainsContainer`: a leading `-` makes the profile list negative,
-- and an input naming nothing matches only a negative list.
function module.contains_container(profile_list: string?, input: string?): boolean
	local negative = false
	if profile_list ~= nil and string.sub(profile_list, 1, 1) == "-" then
		negative = true
		profile_list = string.sub(profile_list, 2)
	end
	if input == nil or input == "" then
		return negative
	end
	return list_names(profile_list, negative, input)
end

return module
