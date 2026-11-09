--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local crypt = require("@harmony/crypt")

local requests = require("./requests")

-- A Lyra id is 21 characters of 6 bits, 126 bits, which fit a GUID's 32 hex digits with two zero
-- bits in front.
local NANOID_ALPHABET = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
local NANOID_LENGTH = 21
local GUID_HEX_LENGTH = 32

local nanoid_char_to_index: { [number]: number } = {}
local nanoid_index_to_byte: { [number]: number } = {}
for i = 1, 64 do
	local byte = string.byte(NANOID_ALPHABET, i)
	nanoid_char_to_index[byte] = i - 1
	nanoid_index_to_byte[i - 1] = byte
end

local HEX_ENCODE = "0123456789abcdef"
local hex_char_to_value: { [number]: number } = {}
for i = 0, 15 do
	hex_char_to_value[string.byte(HEX_ENCODE, i + 1)] = i
end
for i = 0, 5 do
	hex_char_to_value[string.byte("ABCDEF", i + 1)] = 10 + i
end

local function nanoid_to_hex(nanoid: string): string?
	if #nanoid ~= NANOID_LENGTH then
		return nil
	end

	local hex_parts: { string } = {}
	local acc = 0
	local bits = 2

	for i = 1, NANOID_LENGTH do
		local index = nanoid_char_to_index[string.byte(nanoid, i)]
		if index == nil then
			return nil
		end
		acc = bit32.bor(bit32.lshift(acc, 6), index)
		bits += 6
		while bits >= 4 do
			bits -= 4
			local nibble = bit32.band(bit32.rshift(acc, bits), 0xF)
			table.insert(hex_parts, string.sub(HEX_ENCODE, nibble + 1, nibble + 1))
		end
	end

	return table.concat(hex_parts)
end

local function hex_to_nanoid(hex: string): string?
	if #hex ~= GUID_HEX_LENGTH then
		return nil
	end

	local nanoid_parts: { string } = {}
	local acc = bit32.band(hex_char_to_value[string.byte(hex, 1)] or 0, 0x3)
	local bits = 2

	for i = 2, GUID_HEX_LENGTH do
		local nibble = hex_char_to_value[string.byte(hex, i)]
		if nibble == nil then
			return nil
		end
		acc = bit32.bor(bit32.lshift(acc, 4), nibble)
		bits += 4
		while bits >= 6 do
			bits -= 6
			local index = bit32.band(bit32.rshift(acc, bits), 0x3F)
			local byte = nanoid_index_to_byte[index]
			if byte == nil then
				return nil
			end
			table.insert(nanoid_parts, string.char(byte))
		end
	end

	if #nanoid_parts ~= NANOID_LENGTH then
		return nil
	end

	return table.concat(nanoid_parts)
end

local module = {
	EMPTY_GUID = string.rep("0", GUID_HEX_LENGTH),
}

-- A GUID as upstream writes one, 32 lowercase hex digits without dashes, from either form.
function module.normalize_guid(value: string?): string?
	local text = requests.trim(value)
	if not text then
		return nil
	end
	local normalized = string.lower((string.gsub(text, "-", "")))
	if #normalized ~= GUID_HEX_LENGTH or not string.match(normalized, "^[0-9a-f]+$") then
		return nil
	end
	return normalized
end

function module.new_guid(): string
	return crypt.random(16)
end

-- The GUID a Lyra id is known by.
function module.encode(id: string?): string?
	return if id then nanoid_to_hex(id) else nil
end

-- The Lyra id a GUID encodes.
function module.decode(value: string?): string?
	local guid = module.normalize_guid(value)
	return if guid then hex_to_nanoid(guid) else nil
end

-- The Lyra ids in a comma-separated or array list, each once, skipping what isn't one.
function module.decode_list(raw: (string | { string })?): { string }
	local values: { string }
	if type(raw) == "string" then
		values = {}
		for _, part in requests.split_csv_preserving_quotes(raw) do
			table.insert(values, requests.unquote(part) or "")
		end
	elseif type(raw) == "table" then
		values = raw
	else
		return {}
	end
	local result: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, value in values do
		local id = module.decode(value)
		if id and not seen[id] then
			seen[id] = true
			table.insert(result, id)
		end
	end
	return result
end

-- The Lyra ids in a list as given, repeats included, or nil when any entry isn't one.
function module.decode_list_exact(raw: string | { string }): { string }?
	local values = if type(raw) == "string" then requests.split_csv_preserving_quotes(raw) else raw
	local result: { string } = {}
	for _, value in values do
		local id = module.decode(requests.unquote(value))
		if not id then
			return nil
		end
		table.insert(result, id)
	end
	return result
end

return module
