--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

type ImageFormat = "jpg" | "png" | "webp"

local module = {}

function module.trim(value: string?): string?
	if type(value) ~= "string" then
		return nil
	end

	local trimmed = string.gsub(value, "^%s*(.-)%s*$", "%1")
	if trimmed == "" then
		return nil
	end

	return trimmed
end

function module.unquote(value: string?): string?
	local text = module.trim(value)
	if not text then
		return nil
	end

	if string.sub(text, 1, 1) == '"' and string.sub(text, -1) == '"' and #text >= 2 then
		return string.sub(text, 2, -2)
	end

	return text
end

function module.split_csv_preserving_quotes(raw: string): { string }
	local parts: { string } = {}
	local current_parts: { string } = {}
	local in_quotes = false

	for index = 1, #raw do
		local char = string.sub(raw, index, index)
		if char == '"' then
			in_quotes = not in_quotes
			table.insert(current_parts, char)
		elseif char == "," and not in_quotes then
			table.insert(parts, table.concat(current_parts))
			current_parts = {}
		else
			table.insert(current_parts, char)
		end
	end

	local final = table.concat(current_parts)
	if final ~= "" then
		table.insert(parts, final)
	end

	return parts
end

local function lower_or_empty(value: string | number | boolean?): string
	if value == nil then
		return ""
	end

	return string.lower(tostring(value))
end

local function parse_host_without_port(raw_host: string?): string?
	local host = module.trim(raw_host)
	if not host then
		return nil
	end

	if string.sub(host, 1, 1) == "[" then
		local close_bracket = string.find(host, "]", 1, true)
		if close_bracket then
			local ipv6 = string.sub(host, 2, close_bracket - 1)
			return lower_or_empty(ipv6)
		end
	end

	local ipv4_or_name = string.match(host, "^[^:]+")
	return lower_or_empty(ipv4_or_name or host)
end

local function is_private_ipv4(host: string): boolean
	local a, b = string.match(host, "^(%d+)%.(%d+)%.%d+%.%d+$")
	if not a or not b then
		return false
	end

	local first = tonumber(a)
	local second = tonumber(b)
	if not first or not second then
		return false
	end

	if first == 10 or first == 127 then
		return true
	end
	if first == 192 and second == 168 then
		return true
	end
	if first == 172 and second >= 16 and second <= 31 then
		return true
	end
	if first == 169 and second == 254 then
		return true
	end

	return false
end

function module.is_local_network_request(ctx: api.ApiContext?): boolean
	local request = if ctx then ctx.request else nil
	local headers = if request then request.headers else nil
	if type(headers) ~= "table" then
		return false
	end

	local host = parse_host_without_port(headers.host or headers.Host)
	if not host or host == "" then
		return false
	end

	if host == "localhost" or host == "::1" then
		return true
	end
	if is_private_ipv4(host) then
		return true
	end
	if string.sub(host, -6) == ".local" then
		return true
	end

	return false
end

local function query_lookup(query: api.ApiQueryParams?, keys: { string }): string?
	if type(query) ~= "table" then
		return nil
	end

	for _, key in keys do
		local values = query[key]
		if values ~= nil and #values > 0 then
			return values[1]
		end
	end

	local normalized_keys: { [string]: boolean } = {}
	for _, key in keys do
		normalized_keys[string.lower(key)] = true
	end

	for key, values in query do
		if normalized_keys[string.lower(tostring(key))] and #values > 0 then
			return values[1]
		end
	end

	return nil
end

function module.parse_query_string(query: api.ApiQueryParams?, keys: { string }, default_value: string?): string?
	local value = query_lookup(query, keys)
	if value == nil then
		return default_value
	end

	return module.trim(tostring(value)) or default_value
end

function module.parse_query_int(
	query: api.ApiQueryParams?,
	keys: { string },
	default_value: number?,
	min_value: number?,
	max_value: number?
): number?
	local value = query_lookup(query, keys)
	if value == nil then
		return default_value
	end

	local number: number? = nil
	if type(value) == "number" then
		number = value
	elseif type(value) == "string" then
		number = tonumber(module.trim(value) or "")
	end

	if number == nil then
		return default_value
	end

	local integer = math.floor(number)
	if min_value ~= nil and integer < min_value then
		return default_value
	end
	if max_value ~= nil and integer > max_value then
		return default_value
	end

	return integer
end

function module.normalize_image_format(raw_format: string?): ImageFormat?
	local text = module.trim(raw_format)
	if not text then
		return nil
	end

	local normalized = string.lower(text)
	if normalized == "jpg" or normalized == "png" or normalized == "webp" then
		return normalized
	end

	return nil
end

return module
