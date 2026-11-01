--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local module = {}

-- An ISO 8601 UTC time, to the second.
function module.iso8601(unix_ms: number?): string?
	if unix_ms == nil then
		return nil
	end
	return os.date("!%Y-%m-%dT%H:%M:%SZ", math.floor(unix_ms / 1000)) :: string
end

function module.seconds(duration_ms: number?): number?
	return if duration_ms then math.floor(duration_ms / 1000) else nil
end

-- A `YYYY`, `YYYY-MM` or `YYYY-MM-DD` date as an `ItemDate`.
function module.item_date(date: string?): { year: number, month: number?, day: number? }?
	if not date then
		return nil
	end
	local year, month, day = string.match(date, "^(%d%d%d%d)%-?(%d?%d?)%-?(%d?%d?)")
	if not year then
		return nil
	end
	return {
		year = tonumber(year) :: number,
		month = if month ~= "" then tonumber(month) else nil,
		day = if day ~= "" then tonumber(day) else nil,
	}
end

function module.year(date: string?): number?
	local item_date = module.item_date(date)
	return if item_date then item_date.year else nil
end

return module
