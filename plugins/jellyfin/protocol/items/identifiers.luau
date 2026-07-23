--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local ids = require("@lyra/ids")

local constants = require("../../constants")
local encoding = require("../../encoding")
local requests = require("./requests")

local ITEM_ID_HEX_LENGTH = constants.ITEM_ID_HEX_LENGTH
local ITEM_ID_HEX_PATTERN = constants.ITEM_ID_HEX_PATTERN
local NANOID_LENGTH = 21

local module = {}

module.nanoid_to_hex = encoding.nanoid_to_hex
module.hex_to_nanoid = encoding.hex_to_nanoid

function module.encode_jellyfin_item_id(value: number | string?): string?
	if value == nil then
		return nil
	end

	if type(value) == "string" then
		if #value == NANOID_LENGTH then
			return module.nanoid_to_hex(value)
		end
		return nil
	end

	if type(value) == "number" then
		local nanoid = ids.get_id(value)
		if nanoid then
			return module.nanoid_to_hex(nanoid)
		end
		return nil
	end

	return nil
end

local function hex_to_nanoid_safe(value: string?): string?
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

function module.decode_jellyfin_item_id(value: string?): number?
	local nanoid = hex_to_nanoid_safe(value)
	if nanoid then
		return ids.get_db_id(nanoid)
	end
	return nil
end

local function batch_decode_hex_ids(hex_values: { string }): { number }
	local nanoids: { string } = {}
	local seen: { [string]: boolean } = {}
	for _, raw_hex in hex_values do
		local nanoid = hex_to_nanoid_safe(raw_hex)
		if nanoid and not seen[nanoid] then
			seen[nanoid] = true
			table.insert(nanoids, nanoid)
		end
	end

	if #nanoids == 0 then
		return {}
	end

	local ok, db_id_map = pcall(ids.get_db_ids, nanoids)
	if not ok or type(db_id_map) ~= "table" then
		return {}
	end

	local result: { number } = {}
	for _, nanoid in nanoids do
		local db_id = db_id_map[nanoid]
		if db_id then
			table.insert(result, db_id)
		end
	end
	return result
end

function module.parse_jellyfin_ids_csv(raw_ids: string?): { number }
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
	return batch_decode_hex_ids(values)
end

function module.parse_jellyfin_ids_array(raw_ids: string | { string }?): { number }
	if type(raw_ids) == "string" then
		return module.parse_jellyfin_ids_csv(raw_ids)
	end

	if type(raw_ids) ~= "table" then
		return {}
	end

	return batch_decode_hex_ids(raw_ids)
end

function module.encode_jellyfin_db_ids(db_ids: { number }): { [number]: string? }
	if #db_ids == 0 then
		return {}
	end

	local ok, nanoid_map = pcall(ids.get_ids, db_ids)
	if not ok or type(nanoid_map) ~= "table" then
		return {}
	end

	local result: { [number]: string? } = {}
	for _, db_id in db_ids do
		local nanoid = nanoid_map[db_id]
		if nanoid then
			result[db_id] = module.nanoid_to_hex(nanoid)
		end
	end
	return result
end

return module
