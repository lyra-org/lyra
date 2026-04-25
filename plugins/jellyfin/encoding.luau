--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Nanoid ↔ hex encoding utilities.
-- 21 chars × 6 bits = 126 bits, which fits in 32 hex chars (128 bits)
-- with 2 bits of zero padding at the front.

local NANOID_ALPHABET = "_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
local NANOID_LENGTH = 21
local HEX_LENGTH = 32

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

local module = {}

function module.nanoid_to_hex(nanoid: string): string?
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

function module.hex_to_nanoid(hex: string): string?
	if #hex ~= HEX_LENGTH then
		return nil
	end

	local nanoid_parts: { string } = {}
	local acc = bit32.band(hex_char_to_value[string.byte(hex, 1)] or 0, 0x3)
	local bits = 2

	for i = 2, HEX_LENGTH do
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

return module
