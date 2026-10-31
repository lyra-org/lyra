--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local tracks = require("@lyra/tracks")

local constants = require("../../constants")
local encoding = require("../../encoding")
local requests = require("./requests")

local ITEM_ID_HEX_LENGTH = constants.ITEM_ID_HEX_LENGTH
local ITEM_ID_HEX_PATTERN = constants.ITEM_ID_HEX_PATTERN
local NANOID_LENGTH = 21

local module = {}

module.nanoid_to_hex = encoding.nanoid_to_hex
module.hex_to_nanoid = encoding.hex_to_nanoid

function module.encode_jellyfin_item_id(value: string?): string?
	if value ~= nil and #value == NANOID_LENGTH then
		return module.nanoid_to_hex(value)
	end
	return nil
end

-- The public id a Jellyfin item or user id encodes.
function module.decode_jellyfin_item_id(value: string?): string?
	local text = requests.trim(value)
	if not text then
		return nil
	end

	local normalized = string.gsub(string.lower(text), "-", "")
	if #normalized == ITEM_ID_HEX_LENGTH and string.match(normalized, ITEM_ID_HEX_PATTERN) then
		return module.hex_to_nanoid(normalized)
	end

	return nil
end

local function decode_unique(hex_values: { string }): { string }
	local result: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_hex in hex_values do
		local id = module.decode_jellyfin_item_id(raw_hex)
		if id and not seen[id] then
			seen[id] = true
			table.insert(result, id)
		end
	end
	return result
end

function module.parse_jellyfin_ids_csv(raw_ids: string?): { string }
	if type(raw_ids) ~= "string" then
		return {}
	end

	local values: { string } = {}
	for _, part in requests.split_csv_preserving_quotes(raw_ids) do
		local item = requests.trim(requests.unquote(part))
		if item then
			table.insert(values, item)
		end
	end
	return decode_unique(values)
end

function module.parse_jellyfin_ids_array(raw_ids: string | { string }?): { string }
	if type(raw_ids) == "string" then
		return module.parse_jellyfin_ids_csv(raw_ids)
	end

	if type(raw_ids) ~= "table" then
		return {}
	end

	return decode_unique(raw_ids)
end

-- The ids among `item_ids` that name a track the caller can see, in order.
function module.visible_track_ids(item_ids: { string }): { string }
	if #item_ids == 0 then
		return {}
	end
	local found = tracks.get(item_ids)
	local result: { string } = {}
	for _, id in item_ids do
		if found[id] then
			table.insert(result, id)
		end
	end
	return result
end

-- Keeps order and duplicates; nil when any id is malformed.
function module.decode_jellyfin_ids_exact(raw_ids: string | { string }): { string }?
	local values = if type(raw_ids) == "string" then requests.split_csv_preserving_quotes(raw_ids) else raw_ids
	local result: { string } = {}
	for _, raw_id in values do
		local id = module.decode_jellyfin_item_id(requests.unquote(raw_id))
		if not id then
			return nil
		end
		table.insert(result, id)
	end
	return result
end

return module
