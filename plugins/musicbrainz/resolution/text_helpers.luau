--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json = require("@harmony/json")

local module = {}

type JsonValue = json.JsonValue

export type LookupHints = {
	artist_name: string?,
	release_title: string?,
	year: number | string?,
}

export type LookupHintArtist = {
	artist_name: string?,
	[string]: JsonValue,
}

export type LookupHintTrack = {
	lookup_hints: LookupHints?,
	track_title: string?,
	track_total: number?,
	artists: { LookupHintArtist }?,
	disc: number?,
	track: number?,
	duration_ms: number?,
	[string]: JsonValue,
}

export type LookupHintLibrary = {
	language: string?,
	country: string?,
	[string]: JsonValue,
}

export type LookupHintReleaseContext = {
	lookup_hints: LookupHints?,
	release_title: string?,
	artists: { LookupHintArtist }?,
	tracks: { LookupHintTrack }?,
	library: LookupHintLibrary?,
	library_id: number?,
	[string]: JsonValue,
}

function module.normalize_title(title: string?): string
	if type(title) ~= "string" then
		return ""
	end

	local trimmed = string.gsub(title, "^%s*(.-)%s*$", "%1")
	local result = string.lower(trimmed)
	result = string.gsub(result, "（", "(")
	result = string.gsub(result, "）", ")")
	result = string.gsub(result, "〜", "~")
	result = string.gsub(result, "～", "~")
	result = string.gsub(result, "‐", "-")
	result = string.gsub(result, "‑", "-")
	result = string.gsub(result, "‒", "-")
	result = string.gsub(result, "–", "-")
	result = string.gsub(result, "—", "-")
	result = string.gsub(result, "―", "-")
	result = string.gsub(result, "%s*~%s*", "~")
	result = string.gsub(result, "%s*/%s*", "/")
	result = string.gsub(result, "%s*%(", "(")
	result = string.gsub(result, "%)%s*", ")")
	result = string.gsub(result, "%s*%[explicit%]%s*$", "")
	result = string.gsub(result, "%s*%(explicit%)%s*$", "")
	result = string.gsub(result, "[%.!。！]+%s*$", "")
	return result
end

function module.normalize_overlap_title(title: string?): string
	local result = module.normalize_title(title)

	while true do
		local updated = result
		local trailing_bracket = string.match(updated, "%[(.-)%]%s*$")
		if trailing_bracket then
			updated = string.gsub(updated, "%s*%b[]%s*$", "")
		end
		local trailing_paren = string.match(updated, "%((.-)%)%s*$")
		if trailing_paren then
			local p = string.lower(trailing_paren)
			if
				string.find(p, "^feat")
				or string.find(p, "^ft%.")
				or string.find(p, "^with ")
				or string.find(p, "remix", 1, true)
			then
				updated = string.gsub(updated, "%s*%b()%s*$", "")
			end
		end
		if updated == result then
			break
		end
		result = updated
	end
	return result
end

function module.clean_release_title(title: string?): string
	if type(title) ~= "string" then
		return ""
	end

	local cleaned = title
	while true do
		local new = string.gsub(cleaned, "%s*%[.-%]%s*$", "")
		if new == cleaned then
			new = string.gsub(cleaned, "%s*%(EP%)%s*$", "")
		end
		if new == cleaned then
			new = string.gsub(cleaned, "%s*%(Single%)%s*$", "")
		end
		if new == cleaned then
			break
		end
		cleaned = new
	end
	cleaned = string.gsub(cleaned, "^%s*(.-)%s*$", "%1")
	if cleaned == "" then
		return title
	end
	return cleaned
end

function module.trim_text(value: string?): string
	if type(value) ~= "string" then
		return ""
	end
	local trimmed = string.gsub(value, "^%s*(.-)%s*$", "%1")
	return trimmed
end

function module.release_has_barcode(release: { barcode: string? }?): boolean
	if type(release) ~= "table" then
		return false
	end
	return module.trim_text(release.barcode) ~= ""
end

function module.is_blank_text(value: string?): boolean
	return module.trim_text(value) == ""
end

function module.is_placeholder_text(value: string?): boolean
	if type(value) ~= "string" then
		return false
	end
	local trimmed = module.trim_text(value)
	if trimmed == "" then
		return true
	end
	if string.match(trimmed, "^[%?？]+$") then
		return true
	end
	if string.match(trimmed, "^[%s%?？%(%)%[%]%-_]+$") then
		return true
	end
	return false
end

local function normalize_lookup_hint_text(value: string?): string?
	if type(value) ~= "string" then
		return nil
	end
	local normalized = module.trim_text(value)
	if normalized == "" then
		return nil
	end
	return normalized
end

local function normalize_lookup_hint_year(value: number | string?): number?
	local year: number? = nil
	if type(value) == "number" then
		year = value
	elseif type(value) == "string" then
		year = tonumber(value)
	end
	if year and year >= 1900 and year <= 2100 then
		return math.floor(year)
	end
	return nil
end

local function unpack_lookup_hints(hints: LookupHints?): (string?, string?, number?)
	if type(hints) ~= "table" then
		return nil, nil, nil
	end
	return normalize_lookup_hint_text(hints.artist_name),
		normalize_lookup_hint_text(hints.release_title),
		normalize_lookup_hint_year(hints.year)
end

local function infer_lookup_hints_from_tracks(tracks: { LookupHintTrack }?): (string?, string?, number?)
	local inferred_year: number? = nil
	if tracks == nil then
		return nil, nil, nil
	end
	for _, track in tracks do
		local artist_hint, release_hint, track_year = unpack_lookup_hints(track.lookup_hints)
		if not inferred_year and track_year then
			inferred_year = track_year
		end
		if artist_hint or release_hint then
			return artist_hint, release_hint, inferred_year
		end
	end
	return nil, nil, inferred_year
end

function module.infer_lookup_hints(release_context: LookupHintReleaseContext?): (string?, string?, number?)
	local artist_hint, release_hint, inferred_year =
		unpack_lookup_hints(if release_context then release_context.lookup_hints else nil)
	if artist_hint or release_hint or inferred_year then
		return artist_hint, release_hint, inferred_year
	end
	return infer_lookup_hints_from_tracks(if release_context then release_context.tracks else nil)
end

function module.is_generic_artist(name: string?): boolean
	if type(name) ~= "string" then
		return false
	end
	local lower = string.lower(name)
	if lower == "various" or lower == "various artists" then
		return true
	end
	if lower == "unknown" or lower == "[unknown]" or lower == "unknown artist" then
		return true
	end
	if string.match(lower, "^%?+$") then
		return true
	end
	return false
end

function module.contains_multibyte(s: string?): boolean
	if type(s) ~= "string" then
		return false
	end
	for i = 1, #s do
		if string.byte(s, i) >= 0x80 then
			return true
		end
	end
	return false
end

function module.normalize_artist_name_for_lookup(name: string?): string
	if type(name) ~= "string" then
		return ""
	end
	local normalized = string.lower(name)
	normalized = string.gsub(normalized, "[%s%p]+", "")
	return normalized
end

function module.artist_name_matches(local_name: string?, candidate_name: string?): boolean
	if type(local_name) ~= "string" or type(candidate_name) ~= "string" then
		return false
	end
	local local_lower = string.lower(local_name)
	local candidate_lower = string.lower(candidate_name)
	if local_lower == candidate_lower then
		return true
	end
	if string.find(local_lower, candidate_lower, 1, true) then
		return true
	end
	local norm_local = module.normalize_artist_name_for_lookup(local_name)
	local norm_candidate = module.normalize_artist_name_for_lookup(candidate_name)
	if norm_local == "" or norm_candidate == "" then
		return false
	end
	if norm_local == norm_candidate then
		return true
	end
	if #norm_candidate >= 2 and string.find(norm_local, norm_candidate, 1, true) then
		return true
	end
	return false
end

return module
