local apply = require("../../resolution/apply")
local local_release = require("../../resolution/local_release")
local metadata = require("@lyra/metadata")

local release_credits: { any }? = nil
local read_ids: { string } = {}
local stored_created_at_ms: { [string]: number } = {}
local function noop() end
apply.set_provider(setmetatable({
	layer = function(_self: any, node_id: string): any
		return setmetatable({
			set_credits = function(_layer: any, role: any, credits: { any })
				if node_id == "release" and role == metadata.CreditType.Artist then
					release_credits = credits
				end
			end,
		}, {
			__index = function()
				return noop
			end,
		})
	end,
}, {
	__index = function()
		return noop
	end,
}) :: any)
apply.set_artist_reader(function(ids: { string }): { [string]: any }
	local found = {}
	for _, id in ids do
		table.insert(read_ids, id)
		local created_at_ms = stored_created_at_ms[id]
		if created_at_ms ~= nil then
			found[id] = { id = id, artist_name = id, scan_name = id, created_at_ms = created_at_ms }
		end
	end
	return found
end)

-- Two local artists whose names both match the one credited artist are both
-- tagged with it; the credit goes to one of them.
local function credited(artists: { any }): string
	release_credits = nil
	table.clear(read_ids)
	local info = local_release.from_context({
		id = "release",
		release_title = "The Album",
		artists = artists,
		tracks = {},
		library = { language = "eng", country = "XW" },
	} :: any)
	local release = {
		id = "mb-release",
		title = "The Album",
		date = "2020-01-01",
		["artist-credit"] = { { name = "Amy", artist = { id = "mb-amy", name = "Amy" } } },
	}
	assert(apply.process_release(release :: any, info), "the release applies")
	local credits = release_credits
	assert(credits ~= nil and #credits == 1, "the release has one artist credit")
	return credits[1].artist_id
end

assert(credited({
	{ id = "a1", artist_name = "Amy", created_at_ms = 20 },
	{ id = "a2", artist_name = "amy", created_at_ms = 10 },
}) == "a2", "the artist created first gets the credit")
assert(#read_ids == 0, "creation times the context carries are not read")
assert(credited({
	{ id = "a2", artist_name = "Amy", created_at_ms = 10 },
	{ id = "a1", artist_name = "amy", created_at_ms = 10 },
}) == "a1", "artists created together go by id")
assert(credited({
	{ id = "a1", artist_name = "Amy" },
	{ id = "a2", artist_name = "amy", created_at_ms = 10 },
}) == "a2", "an artist without a known creation time goes after one with it")
assert(table.find(read_ids, "a1") ~= nil, "the unknown creation time is read")

stored_created_at_ms["a2"] = 5
assert(credited({
	{ id = "a1", artist_name = "Amy", created_at_ms = 10 },
	{ id = "a2", artist_name = "amy" },
}) == "a2", "a creation time read for the tie counts")
