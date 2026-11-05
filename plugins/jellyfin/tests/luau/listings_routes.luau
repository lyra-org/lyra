local fixture, harness = ...
local serde = require("@harmony/serde")

local catalog = require("../../protocol/items/catalog")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local listings = require("../../routes/listings")
local options = require("../../protocol/items/options")
local playlist_access = require("../../protocol/playlist_access")
local results = require("../../protocol/items/results")

-- The listings that grow with the library stream their result, byte for byte what the whole
-- result encodes to, whatever the batches it streams in.

local ids = fixture.ids
local AUTHORIZATION = `MediaBrowser Token="{fixture.api_keys.admin_key}"`

local function hex(id: string): string
	return item_ids.encode_jellyfin_item_id(id) :: string
end

local function raw(params: { [string]: string }): { [string]: { string } }
	local query: { [string]: { string } } = {}
	for key, value in params do
		query[key] = { value }
	end
	return query
end

local function parse(params: { [string]: string }): options.ItemsQueryOptions
	return options.for_user(options.parse(raw(params) :: any), fixture.users.admin)
end

local function call(handler: (any) -> any, path: string, params: { [string]: string }, route_params: any): any
	return harness.plugin:call(
		handler,
		{
			plugin_id = "jellyfin",
			request = {
				method = "GET",
				path = path,
				headers = { authorization = AUTHORIZATION },
				query = raw(params),
				body_raw = "",
				json = nil,
			},
			params = route_params or {},
		} :: any
	)
end

local function streamed(response: any, label: string): string
	assert(response.kind == "stream", `{label}: {response.kind}`)
	assert(response.status == 200, `{label}: {response.status}`)
	assert(response.headers["content-type"] == "application/json", label)
	return harness.plugin:read_body(response)
end

local function query_result(list: () -> ({ any }, number, number)): string
	local items, total, start_index = list()
	return serde.json.encode(catalog.build_item_query_result(items, total, start_index))
end

-- Each route, what it answered whole before it streamed, and what it answers now: in one batch,
-- and in batches of a single item.
local cases = {
	{
		label = "/Artists",
		handler = listings.handle_artists,
		params = { SortBy = "SortName" },
		whole = function(params)
			return query_result(function()
				return listing.list_artist_items(parse(params), nil)
			end)
		end,
	},
	{
		label = "/Artists paged",
		handler = listings.handle_artists,
		params = { StartIndex = "1", Limit = "2" },
		whole = function(params)
			return query_result(function()
				return listing.list_artist_items(parse(params), nil)
			end)
		end,
	},
	{
		label = "/Artists/AlbumArtists",
		handler = listings.handle_album_artists,
		params = {},
		whole = function(params)
			return query_result(function()
				return listing.list_artist_items(parse(params), "release")
			end)
		end,
	},
	{
		label = "/Persons",
		handler = listings.handle_persons,
		params = {},
		whole = function(params)
			return query_result(function()
				return listing.list_person_items(parse(params))
			end)
		end,
	},
	{
		label = "/Genres",
		handler = listings.handle_genres,
		params = {},
		whole = function(params)
			return query_result(function()
				return listing.list_genre_items(parse(params))
			end)
		end,
	},
	{
		label = "/Playlists/{id}/Items",
		handler = listings.handle_playlist_items,
		params = {},
		route_params = { playlistId = hex(ids.mix) },
		whole = function(params)
			return query_result(function()
				return listing.list_playlist_track_items(playlist_access.find(ids.mix), parse(params))
			end)
		end,
	},
	{
		label = "/Items/Latest",
		handler = listings.handle_latest,
		params = { IncludeItemTypes = "Audio", GroupItems = "false" },
		whole = function(params)
			return serde.json.encode(catalog.items_to_json(listing.list_latest_items(parse(params))))
		end,
	},
}

local default_batch = results.BATCH_SIZE
for _, case in cases do
	local whole = case.whole(case.params)
	local decoded = serde.json.decode(whole) :: any
	local listed = if decoded.Items then #decoded.Items else #decoded
	assert(listed > 0, `{case.label} lists nothing to stream`)
	for _, size in { default_batch, 1 } do
		results.BATCH_SIZE = size
		local body = streamed(call(case.handler, case.label, case.params, case.route_params), case.label)
		assert(body == whole, `{case.label} in batches of {size}\nstreamed\n{body}\nwhole\n{whole}`)
	end
	results.BATCH_SIZE = default_batch
end

-- Similar items come in a random order, so only their shape is compared.
local similar = call(listings.handle_similar, "/Items/{id}/Similar", {}, { itemId = hex(ids.solo) })
local similar_result = serde.json.decode(streamed(similar, "/Items/{id}/Similar")) :: any
assert(similar_result.StartIndex == 0 and similar_result.TotalRecordCount ~= nil, "similar result")
local missing = call(
	listings.handle_similar,
	"/Items/{id}/Similar",
	{},
	{ itemId = "00000000000000000000000000000000" }
)
assert(missing.kind ~= "stream" and missing.status == 404, `similar of a missing item: {missing.status}`)

-- A listing past what one encode may hold still streams whole.
local artist = listing.list_artist_items(parse({}), nil)[1]
local copies = math.ceil(serde.MAX_ENCODED_BYTES / #serde.json.encode(catalog.item_to_json(artist))) + 1
local batch = table.create(results.BATCH_SIZE, artist)
local response = harness.plugin:call(function()
	return catalog.item_query_response(function(emit)
		for _ = 1, math.ceil(copies / results.BATCH_SIZE) do
			emit(batch)
		end
		return copies, 0
	end)
end)
local body = streamed(response, "a listing past one encode")
assert(#body > serde.MAX_ENCODED_BYTES, `{#body} bytes`)
assert(string.sub(body, 1, 10) == '{"Items":[', string.sub(body, 1, 10))
assert(string.find(body, `"StartIndex":0,"TotalRecordCount":{copies}` .. "}$"), string.sub(body, -80))
local whole_items = table.create(copies, catalog.item_to_json(artist))
local ok, err = pcall(serde.json.encode, results.build_query_result(whole_items, copies, 0))
assert(not ok and string.find(tostring(err), "encodes to more than", 1, true), tostring(err))
