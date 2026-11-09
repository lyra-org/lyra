local fixture = ...

local filters = require("../../protocol/items/filters")
local item_ids = require("../../protocol/items/identifiers")
local options = require("../../protocol/items/options")

local open_library = item_ids.encode(fixture.libraries.open) :: string
local closed_library = item_ids.encode(fixture.libraries.closed) :: string
local unknown_item = item_ids.encode("NoSuchItemAnywhere000") :: string

local function collect(params: { [string]: string }): filters.FilterValues?
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return filters.collect(options.for_user(options.parse(raw :: any), fixture.users.listener))
end

local function joined(values: { any }, field: string?): string
	local listed = {}
	for _, value in values do
		table.insert(listed, tostring(if field then value[field] else value))
	end
	return table.concat(listed, ",")
end

-- `/Items/Filters` as `genres|tags|years`.
local function legacy(params: { [string]: string }): string
	local json = filters.legacy_json(collect(params)) :: any
	return `{joined(json.Genres)}|{joined(json.Tags)}|{joined(json.Years)}`
end

-- `/Items/Filters2` as `genres|tags`, for a single music item type.
local function current(params: { [string]: string }): string
	local json = filters.to_json(collect(params), { params.IncludeItemTypes }) :: any
	return `{joined(json.Genres, "Name")}|{joined(json.Tags)}`
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

-- Only the library the caller sees lends its genres, tags and years.
check(legacy({}), "Rock|Vinyl|2001", "every item type")
check(legacy({ ParentId = open_library }), "Rock|Vinyl|2001", "under the library the caller sees")
check(legacy({ ParentId = closed_library }), "||", "under the library the caller can't see")
check(legacy({ ParentId = unknown_item }), "||", "under an id naming nothing, as under a hidden one")
check(legacy({ IncludeItemTypes = "MusicArtist" }), "||", "the hidden library's artist lends no tag")

for _, item_type in { "MusicAlbum", "Audio", "MusicArtist" } do
	local expected = if item_type == "MusicArtist" then "|" else "Rock|Vinyl"
	check(current({ IncludeItemTypes = item_type }), expected, `{item_type} filters`)
	check(
		current({ IncludeItemTypes = item_type, ParentId = open_library }),
		expected,
		`{item_type} filters under the library the caller sees`
	)
	check(
		current({ IncludeItemTypes = item_type, ParentId = closed_library }),
		"|",
		`{item_type} filters under the library the caller can't see`
	)
end
