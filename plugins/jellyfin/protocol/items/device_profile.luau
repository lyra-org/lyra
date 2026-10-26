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

local CODEC_TYPES: { [number]: string } = { [0] = "Video", "VideoAudio", "Audio" }
local CONDITION_TYPES: { [number]: string } =
	{ [0] = "Equals", "NotEquals", "LessThanEqual", "GreaterThanEqual", "EqualsAny" }
local CONDITION_PROPERTIES: { [number]: string } = {
	[0] = "AudioChannels",
	[1] = "AudioBitrate",
	[2] = "AudioProfile",
	[3] = "Width",
	[4] = "Height",
	[5] = "Has64BitOffsets",
	[6] = "PacketLength",
	[7] = "VideoBitDepth",
	[8] = "VideoBitrate",
	[9] = "VideoFramerate",
	[10] = "VideoLevel",
	[11] = "VideoProfile",
	[12] = "VideoTimestamp",
	[13] = "IsAnamorphic",
	[14] = "RefFrames",
	[16] = "NumAudioStreams",
	[17] = "NumVideoStreams",
	[18] = "IsSecondaryAudio",
	[19] = "VideoCodecTag",
	[20] = "IsAvc",
	[21] = "IsInterlaced",
	[22] = "AudioSampleRate",
	[23] = "AudioBitDepth",
	[24] = "VideoRangeType",
	[25] = "NumStreams",
	[26] = "VideoRotation",
}

export type Condition = {
	condition: string,
	property: string,
	value: string?,
	is_required: boolean,
}

local function parse_conditions(profile: JsonObject, key: string): ({ Condition }?, string?)
	local conditions: { Condition } = {}
	for _, raw in module.object_list(profile, key) do
		local condition = enum_value(raw.Condition, CONDITION_TYPES)
		if condition == nil then
			return nil, `{key}: invalid Condition {tostring(raw.Condition)}`
		end
		local property = enum_value(raw.Property, CONDITION_PROPERTIES)
		if property == nil then
			return nil, `{key}: invalid Property {tostring(raw.Property)}`
		end
		table.insert(conditions, {
			condition = condition,
			property = property,
			value = module.string_field(raw, "Value"),
			-- Upstream's conditions are required unless they say otherwise.
			is_required = raw.IsRequired ~= false,
		})
	end
	return conditions, nil
end

export type CodecProfile = {
	type: string,
	codec: string?,
	container: string?,
	conditions: { Condition },
	apply_conditions: { Condition },
}

-- The profile's codec profiles, or the error upstream's deserializer answers with a 400.
function module.codec_profiles(device_profile: JsonObject): ({ CodecProfile }?, string?)
	local profiles: { CodecProfile } = {}
	for _, raw in module.object_list(device_profile, "CodecProfiles") do
		local codec_type = enum_value(raw.Type, CODEC_TYPES)
		if codec_type == nil then
			return nil, `CodecProfiles: invalid Type {tostring(raw.Type)}`
		end
		local conditions, conditions_err = parse_conditions(raw, "Conditions")
		if conditions == nil then
			return nil, `CodecProfiles {conditions_err}`
		end
		local apply_conditions, apply_err = parse_conditions(raw, "ApplyConditions")
		if apply_conditions == nil then
			return nil, `CodecProfiles {apply_err}`
		end
		table.insert(profiles, {
			type = codec_type,
			codec = module.string_field(raw, "Codec"),
			container = module.string_field(raw, "Container"),
			conditions = conditions,
			apply_conditions = apply_conditions,
		})
	end
	return profiles, nil
end

export type AudioProperties = {
	channels: number?,
	bitrate: number?,
	sample_rate: number?,
	bit_depth: number?,
}

-- Upstream's `ConditionProcessor.IsAudioConditionSatisfied`. A condition on anything but audio is
-- an error, which upstream answers with a 400.
local function audio_condition_satisfied(condition: Condition, audio: AudioProperties): (boolean, string?)
	local property = condition.property
	local current: number?
	if property == "AudioBitrate" then
		current = audio.bitrate
	elseif property == "AudioChannels" then
		current = audio.channels
	elseif property == "AudioSampleRate" then
		current = audio.sample_rate
	elseif property == "AudioBitDepth" then
		current = audio.bit_depth
	else
		return false, `Unexpected condition on audio file: {property}`
	end

	-- An unknown value satisfies a condition that isn't required.
	if current == nil then
		return not condition.is_required, nil
	end
	if condition.condition == "EqualsAny" then
		for _, part in string.split(condition.value or "", "|") do
			if module.parse_int(part) == current then
				return true, nil
			end
		end
		return false, nil
	end
	local expected = module.parse_int(condition.value)
	if expected == nil then
		return false, nil
	elseif condition.condition == "Equals" then
		return current == expected, nil
	elseif condition.condition == "NotEquals" then
		return current ~= expected, nil
	elseif condition.condition == "LessThanEqual" then
		return current <= expected, nil
	end
	return current >= expected, nil
end

-- Upstream's `StreamBuilder.GetProfileConditionsForAudio`: the conditions of every audio codec
-- profile for the codec and container whose apply conditions the audio meets; with
-- `failed_only`, only those the audio fails.
function module.audio_conditions(
	profiles: { CodecProfile },
	container: string?,
	codec: string?,
	audio: AudioProperties,
	failed_only: boolean
): ({ Condition }?, string?)
	local conditions: { Condition } = {}
	for _, profile in profiles do
		-- Unlike containers, a codec list here is never negative.
		if
			profile.type == "Audio"
			and module.contains_container(profile.container, container)
			and list_names(profile.codec, false, codec or "")
		then
			local applies = true
			for _, apply_condition in profile.apply_conditions do
				local satisfied, err = audio_condition_satisfied(apply_condition, audio)
				if err then
					return nil, err
				elseif not satisfied then
					applies = false
					break
				end
			end
			if applies then
				for _, condition in profile.conditions do
					local satisfied, err = false, nil
					if failed_only then
						satisfied, err = audio_condition_satisfied(condition, audio)
					end
					if err then
						return nil, err
					elseif not satisfied then
						table.insert(conditions, condition)
					end
				end
			end
		end
	end
	return conditions, nil
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
