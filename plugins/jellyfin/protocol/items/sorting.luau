--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

type SortValueKind = "number" | "string"
type SortValue = number | string
type SortableItem = {
	ParentIndexNumber: number?,
	IndexNumber: number?,
	Name: string?,
	Artists: { string }?,
	AlbumArtist: string?,
	Album: string?,
	Id: string?,
	RunTimeTicks: number?,
	DateCreated: string?,
	PremiereDate: string?,
	CommunityRating: number?,
	SortName: string?,
}

export type Options = {
	sort_by: { string },
	sort_order: string?,
}

local module = {}

local function lower_or_empty(value: string | number | boolean?): string
	if value == nil then
		return ""
	end
	return string.lower(tostring(value))
end
local function sort_tokens_from_options(options: Options): { string }
	if #options.sort_by == 0 then
		return { "sortname" }
	end

	local normalized: { string } = {}
	for _, token: string in options.sort_by do
		local normalized_token = lower_or_empty(token)
		if normalized_token ~= "" then
			table.insert(normalized, normalized_token)
		end
	end

	if #normalized == 0 then
		return { "sortname" }
	end

	return normalized
end

local function sort_value_for_item(item: SortableItem?, sort_token: string): (SortValueKind, SortValue?)
	if sort_token == "parentindexnumber" then
		return "number", tonumber(if item then item.ParentIndexNumber else nil)
	end
	if sort_token == "indexnumber" then
		return "number", tonumber(if item then item.IndexNumber else nil)
	end
	if sort_token == "name" then
		return "string", lower_or_empty(if item then item.Name else nil)
	end
	if sort_token == "albumartist" then
		local artists = if item then item.Artists else nil
		local first_artist = if type(artists) == "table" then artists[1] else nil
		return "string", lower_or_empty(if item then (item.AlbumArtist or first_artist or item.Name) else nil)
	end
	if sort_token == "artist" then
		-- Per-track Artists first; AlbumArtist would collapse compilation tracks under one credit.
		local artists = if item then item.Artists else nil
		local first_artist = if type(artists) == "table" then artists[1] else nil
		return "string", lower_or_empty(if item then (first_artist or item.AlbumArtist or item.Name) else nil)
	end
	if sort_token == "release" then
		return "string", lower_or_empty(if item then (item.Album or item.Name) else nil)
	end
	if sort_token == "id" then
		return "string", lower_or_empty(if item then item.Id else nil)
	end
	if sort_token == "runtime" then
		return "number", tonumber(if item then item.RunTimeTicks else nil)
	end
	if sort_token == "datecreated" then
		return "string", if item then item.DateCreated else nil
	end
	if sort_token == "premieredate" then
		return "string", if item then item.PremiereDate else nil
	end
	if sort_token == "random" then
		return "number", 0
	end
	if sort_token == "communityrating" then
		return "number", tonumber(if item then item.CommunityRating else nil)
	end
	if sort_token == "dateplayed" then
		return "string", if item then item.DateCreated else nil
	end
	if sort_token == "playcount" then
		return "number", 0
	end

	return "string", lower_or_empty(if item then (item.SortName or item.Name) else nil)
end

local function compare_sort_values(
	a_kind: SortValueKind,
	a_value: SortValue?,
	b_kind: SortValueKind,
	b_value: SortValue?,
	descending: boolean
): number
	if a_value == nil and b_value == nil then
		return 0
	end
	if a_value == nil then
		return 1
	end
	if b_value == nil then
		return -1
	end

	if a_kind ~= b_kind then
		a_value = tostring(a_value)
		b_value = tostring(b_value)
	end

	if a_value == b_value then
		return 0
	end

	local comparison: number
	if a_kind == "number" and b_kind == "number" then
		local a_number = tonumber(a_value)
		local b_number = tonumber(b_value)
		if a_number == nil or b_number == nil then
			comparison = 0
		else
			comparison = if a_number < b_number then -1 else 1
		end
	else
		local a_text = tostring(a_value)
		local b_text = tostring(b_value)
		comparison = if a_text < b_text then -1 else 1
	end
	if descending then
		comparison *= -1
	end
	return comparison
end

function module.shuffle<T>(items: { T })
	for i = #items, 2, -1 do
		local j = math.random(1, i)
		items[i], items[j] = items[j], items[i]
	end
end

function module.tokens(options: Options): { string }
	return sort_tokens_from_options(options)
end

function module.apply<T>(items: { T }, options: Options)
	local sort_tokens = module.tokens(options)
	local descending = lower_or_empty(options.sort_order) == "descending"

	if sort_tokens[1] == "random" then
		module.shuffle(items)
		return
	end

	local token_count = #sort_tokens

	-- Precompute sort keys to avoid re-extracting on every comparison.
	-- For each item, store kinds[1..token_count+2] and values[1..token_count+2]
	-- where +1 = name tiebreaker, +2 = id tiebreaker.
	local all_kinds: { { SortValueKind } } = {}
	local all_values: { { SortValue? } } = {}
	for i, item: T in items do
		local sortable_item = (item :: any) :: SortableItem
		local kinds: { SortValueKind } = {}
		local values: { SortValue? } = {}
		for j, sort_token: string in sort_tokens do
			local kind, value = sort_value_for_item(sortable_item, sort_token)
			kinds[j] = kind
			values[j] = value
		end
		local _, name_value = sort_value_for_item(sortable_item, "name")
		local _, id_value = sort_value_for_item(sortable_item, "id")
		kinds[token_count + 1] = "string"
		values[token_count + 1] = name_value
		kinds[token_count + 2] = "string"
		values[token_count + 2] = id_value
		all_kinds[i] = kinds
		all_values[i] = values
	end

	local indices: { number } = {}
	for i = 1, #items do
		indices[i] = i
	end

	table.sort(indices, function(a_idx: number, b_idx: number): boolean
		local a_kinds = all_kinds[a_idx]
		local a_values = all_values[a_idx]
		local b_kinds = all_kinds[b_idx]
		local b_values = all_values[b_idx]

		for j = 1, token_count do
			local cmp = compare_sort_values(a_kinds[j], a_values[j], b_kinds[j], b_values[j], descending)
			if cmp < 0 then
				return true
			end
			if cmp > 0 then
				return false
			end
		end

		local name_cmp =
			compare_sort_values("string", a_values[token_count + 1], "string", b_values[token_count + 1], false)
		if name_cmp < 0 then
			return true
		end
		if name_cmp > 0 then
			return false
		end

		return compare_sort_values("string", a_values[token_count + 2], "string", b_values[token_count + 2], false) < 0
	end)

	-- Reorder items in-place via sorted indices
	local sorted: { T } = {}
	for i, idx: number in indices do
		sorted[i] = items[idx]
	end
	for i = 1, #sorted do
		items[i] = sorted[i]
	end
end

return module
