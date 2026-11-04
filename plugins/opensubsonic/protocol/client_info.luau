--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local serde = require("@harmony/serde")

local request = require("./request")

-- A client's playback capabilities, as the `transcoding` extension's `ClientInfo` gives them.
-- Names are in lowercase, containers and codecs as Lyra names a source's, and a limit of 0 is no
-- limit. Malformed JSON fails the method; names Lyra doesn't know don't, and simply match nothing.

export type DirectPlayProfile = {
	containers: { string },
	codecs: { string },
	protocols: { string },
	max_channels: number?,
}

export type TranscodingProfile = {
	-- As the client names it, for the delivery format it asks for.
	container: string,
	codec: string,
	protocol: string,
	max_channels: number?,
}

export type Comparison = "equals" | "notequals" | "lessthanequal" | "greaterthanequal"

-- A limitation Lyra can check has a comparison and numbers. The AAC profile, and names,
-- comparisons or values Lyra doesn't know, leave them unset.
export type Limitation = {
	name: string,
	comparison: Comparison?,
	numbers: { number }?,
	required: boolean,
}

export type CodecProfile = {
	codec: string,
	limitations: { Limitation },
}

export type ClientInfo = {
	max_bitrate: number?,
	max_transcoding_bitrate: number?,
	direct_play: { DirectPlayProfile },
	transcoding: { TranscodingProfile },
	codec_profiles: { CodecProfile },
}

local module = {}

-- Clients' container names, by the name Lyra gives a source's container.
local CONTAINERS: { [string]: string } = {
	mpeg = "mp3",
	mpga = "mp3",
	mp4 = "m4a",
	m4b = "m4a",
	m4p = "m4a",
	m4r = "m4a",
	adts = "aac",
	oga = "ogg",
	opus = "ogg",
	wave = "wav",
	aif = "aiff",
	wv = "wavpack",
	mpp = "mpc",
}

-- Clients' codec names, by the name Lyra gives the codec.
local CODECS: { [string]: string } = {
	adts = "aac",
	wv = "wavpack",
	mpc7 = "musepack7",
	mpc8 = "musepack8",
	wmav2 = "wma",
	aif = "aiff",
}

local COMPARISONS: { [string]: Comparison } = {
	equals = "equals",
	notequals = "notequals",
	lessthanequal = "lessthanequal",
	greaterthanequal = "greaterthanequal",
}

local LIMITATIONS: { [string]: string } = {
	audiochannels = "audioChannels",
	audiobitrate = "audioBitrate",
	audiosamplerate = "audioSamplerate",
	audiobitdepth = "audioBitdepth",
	audioprofile = "audioProfile",
}

-- `body`, valid JSON, as it decodes without losing its shape: each object holds the key `""`, so
-- even one empty or of nulls decodes unlike an array, and an array's `null` items are `false`
-- rather than gaps. Nil when an object uses the key `""` itself.
local function preserve_shape(body: string): string?
	local parts: { string } = {}
	local copied = 1
	local position = 1
	local in_array: { boolean } = {}
	while true do
		local found = string.find(body, '["{}%[%]n]', position)
		if not found then
			break
		end
		local char = string.sub(body, found, found)
		position = found + 1
		if char == '"' then
			while true do
				local stop = string.find(body, '["\\]', position) :: number
				local quote = string.sub(body, stop, stop) == '"'
				position = stop + if quote then 1 else 2
				if quote then
					break
				end
			end
			if position == found + 2 and string.match(body, "^[ \t\r\n]*:", position) then
				return nil
			end
		elseif char == "{" then
			local after = string.match(body, "^{[ \t\r\n]*}()", found) :: number?
			table.insert(parts, string.sub(body, copied, found - 1))
			table.insert(parts, if after then '{"":0}' else '{"":0,')
			copied = after or position
			if after then
				position = after
			else
				table.insert(in_array, false)
			end
		elseif char == "[" then
			table.insert(in_array, true)
		elseif char == "}" or char == "]" then
			table.remove(in_array)
		elseif in_array[#in_array] then
			table.insert(parts, string.sub(body, copied, found - 1))
			table.insert(parts, "false")
			copied = found + 4
			position = copied
		else
			position = found + 4
		end
	end
	table.insert(parts, string.sub(body, copied))
	return table.concat(parts)
end

local function invalid(path: string, problem: string): ...any
	return request.fail(0, `Invalid ClientInfo: {path} {problem}`)
end

-- A decoded object holds only string keys; an array only numbers, and an empty array none.
local function object(value: unknown, path: string): { [string]: unknown }
	if type(value) ~= "table" or type((next(value))) ~= "string" then
		return invalid(path, "must be an object")
	end
	return value :: any
end

local function list(value: unknown, path: string): { unknown }
	if value == nil then
		return {}
	end
	if type(value) ~= "table" then
		return invalid(path, "must be an array")
	end
	for key in value :: { [unknown]: unknown } do
		if type(key) ~= "number" then
			return invalid(path, "must be an array")
		end
	end
	return value :: { unknown }
end

local function text(value: unknown, path: string): string
	if value == nil then
		return ""
	end
	if type(value) ~= "string" then
		return invalid(path, "must be a string")
	end
	return string.lower(value)
end

local function texts(value: unknown, path: string, aliases: { [string]: string }?): { string }
	local values: { string } = {}
	for index, item in list(value, path) do
		local name = text(item, `{path}[{index}]`)
		table.insert(values, if aliases then aliases[name] or name else name)
	end
	return values
end

-- A non-negative integer; nil when absent or 0.
local function limit(value: unknown, path: string): number?
	if value == nil then
		return nil
	end
	if type(value) ~= "number" or value < 0 or value ~= math.floor(value) or value == math.huge then
		return invalid(path, "must be a non-negative integer")
	end
	return if value > 0 then value else nil
end

local function codec_name(value: unknown, path: string): string
	local name = text(value, path)
	return CODECS[name] or name
end

local function limitation(value: unknown, path: string): Limitation
	local fields = object(value, path)
	local raw_name = text(fields.name, `{path}.name`)
	local name = LIMITATIONS[raw_name] or raw_name
	local comparison = COMPARISONS[text(fields.comparison, `{path}.comparison`)]
	local values = texts(fields.values, `{path}.values`)
	if fields.required ~= nil and type(fields.required) ~= "boolean" then
		return invalid(`{path}.required`, "must be a boolean")
	end

	local numbers: { number }? = {}
	for _, item in values do
		local number = if string.match(item, "^%d+$") then tonumber(item) else nil
		if not number or not numbers then
			numbers = nil
			break
		end
		table.insert(numbers, number)
	end
	local checkable = comparison and numbers and #numbers > 0 and LIMITATIONS[raw_name] and name ~= "audioProfile"
	return {
		name = name,
		comparison = if checkable then comparison else nil,
		numbers = if checkable then numbers else nil,
		required = fields.required == true,
	}
end

-- The `ClientInfo` a JSON `body` holds; fails the method when it is malformed.
function module.parse(body: string): ClientInfo
	if body == "" then
		return request.fail(0, "The request body must hold the client's ClientInfo as JSON")
	end
	if not pcall(serde.json.decode, body) then
		return request.fail(0, "The request body isn't valid JSON")
	end
	local shaped = preserve_shape(body)
	if not shaped then
		return request.fail(0, 'Invalid ClientInfo: an object uses the empty key ""')
	end
	local fields = object(serde.json.decode(shaped), "ClientInfo")

	local direct_play: { DirectPlayProfile } = {}
	for index, item in list(fields.directPlayProfiles, "directPlayProfiles") do
		local path = `directPlayProfiles[{index}]`
		local profile = object(item, path)
		table.insert(direct_play, {
			containers = texts(profile.containers, `{path}.containers`, CONTAINERS),
			codecs = texts(profile.audioCodecs, `{path}.audioCodecs`, CODECS),
			protocols = texts(profile.protocols, `{path}.protocols`),
			max_channels = limit(profile.maxAudioChannels, `{path}.maxAudioChannels`),
		})
	end

	local transcoding: { TranscodingProfile } = {}
	for index, item in list(fields.transcodingProfiles, "transcodingProfiles") do
		local path = `transcodingProfiles[{index}]`
		local profile = object(item, path)
		table.insert(transcoding, {
			container = text(profile.container, `{path}.container`),
			codec = codec_name(profile.audioCodec, `{path}.audioCodec`),
			protocol = text(profile.protocol, `{path}.protocol`),
			max_channels = limit(profile.maxAudioChannels, `{path}.maxAudioChannels`),
		})
	end

	-- Profiles of anything but audio codecs are left out.
	local codec_profiles: { CodecProfile } = {}
	for index, item in list(fields.codecProfiles, "codecProfiles") do
		local path = `codecProfiles[{index}]`
		local profile = object(item, path)
		local limitations: { Limitation } = {}
		for limitation_index, entry in list(profile.limitations, `{path}.limitations`) do
			table.insert(limitations, limitation(entry, `{path}.limitations[{limitation_index}]`))
		end
		local codec = codec_name(profile.name, `{path}.name`)
		if text(profile.type, `{path}.type`) == "audiocodec" then
			table.insert(codec_profiles, { codec = codec, limitations = limitations })
		end
	end

	return {
		max_bitrate = limit(fields.maxAudioBitrate, "maxAudioBitrate"),
		max_transcoding_bitrate = limit(fields.maxTranscodingAudioBitrate, "maxTranscodingAudioBitrate"),
		direct_play = direct_play,
		transcoding = transcoding,
		codec_profiles = codec_profiles,
	}
end

return module
