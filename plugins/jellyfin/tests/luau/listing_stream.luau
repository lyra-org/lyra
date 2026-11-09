local fixture, harness = ...
local api = require("@lyra/api")
local auth = require("@lyra/auth")
local catalog = require("../../protocol/items/catalog")
local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local serde = require("@harmony/serde")

local ids = fixture.ids

local function options_for(params: { [string]: string }): options.ItemsQueryOptions
	local raw: { [string]: { string } } = {}
	for key, value in params do
		raw[key] = { value }
	end
	return options.for_user(options.parse(raw :: any), fixture.users.admin)
end

-- `/Items` streams, as it answers, the query result the listing builds whole, byte for byte.
for _, params in
	{
		{ Recursive = "true" },
		{ Recursive = "true", IncludeItemTypes = "Audio", StartIndex = "1", Limit = "1" },
		{ Recursive = "true", IncludeItemTypes = "Audio,MusicAlbum,Playlist", SortBy = "SortName" },
		{ ParentId = item_ids.encode(ids.mix) },
		{ Ids = `{item_ids.encode(ids.s2)},{item_ids.encode(ids.s1)}` },
		{ Recursive = "true", IncludeItemTypes = "MusicArtist", StartIndex = "50" },
	} :: { { [string]: string } }
do
	local items, total, start_index = listing.list_items(options_for(params))
	local whole = serde.json.encode(catalog.build_item_query_result(items, total, start_index))
	-- As `/Items` does, the handler authenticates, and the body is produced for whom it did.
	local response = harness.plugin:call(function()
		assert(auth.resolve_auth(fixture.api_keys.admin_key), "authenticated")
		local listed = options_for(params)
		return api.response.stream(200, function(writer)
			catalog.write_item_query_result(writer, function(emit)
				return listing.each_item_batch(listed, emit)
			end)
		end, { ["content-type"] = "application/json" })
	end)
	assert(response.kind == "stream" and response.status == 200)
	assert(response.headers["content-type"] == "application/json")
	local body = harness.plugin:read_body(response)
	assert(body == whole, `streamed\n{body}\nwhole\n{whole}`)
end
