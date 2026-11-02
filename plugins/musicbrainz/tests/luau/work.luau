local apply = require("../../resolution/apply")
local local_release = require("../../resolution/local_release")
local metadata = require("@lyra/metadata")

local work_ids: { [string]: string } = {}
local ensured: { [string]: any } = {}
local function noop() end
apply.set_provider(setmetatable({
	ensure_artist = function(_self: any, request: any): string?
		ensured[request.id_value] = request
		return nil
	end,
	layer = function(_self: any, node_id: string): any
		return setmetatable({
			set_id = function(_layer: any, id_type: string, id_value: string)
				if id_type == "work_id" then
					work_ids[node_id] = id_value
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

local function applied_work_id(relations: { any }?): string?
	table.clear(work_ids)
	local info = local_release.from_context({
		id = "release",
		release_title = "The Album",
		artists = {},
		tracks = {
			{ id = "track-a", track_title = "Song 1", disc = 1, track = 1 },
			{ id = "track-b", track_title = "Song 2", disc = 1, track = 2 },
		},
		library = { language = "eng", country = "XW" },
	} :: any)
	local release = {
		id = "release",
		title = "The Album",
		date = "2020-01-01",
		media = {
			{
				position = 1,
				["track-count"] = 2,
				tracks = {
					{
						id = "track-1",
						title = "Song 1",
						position = 1,
						recording = { id = "recording-1", relations = relations },
					},
					{ id = "track-2", title = "Song 2", position = 2, recording = { id = "recording-2" } },
				},
			},
		},
	}
	assert(apply.process_release(release :: any, info), "the release applies")
	assert(work_ids["track-b"] == nil, "a recording whose relations weren't fetched keeps its work")
	return work_ids["track-a"]
end

local performance = { ["target-type"] = "work", type = "performance", work = { id = "work" } }
assert(applied_work_id({ performance }) == "work", "a performed work is set")
assert(applied_work_id({}) == "", "relations without a work clear the work")
assert(applied_work_id(nil) == nil, "unfetched relations keep the work")

-- An artist a relation credits is ensured with its type, so its match keeps it.
local vocal =
	{ ["target-type"] = "artist", type = "vocal", artist = { id = "singer", name = "Singer", type = "Person" } }
applied_work_id({ performance, vocal })
local singer = ensured.singer
assert(singer and singer.artist_type == metadata.ArtistType.Person, "the credited artist keeps its type")
