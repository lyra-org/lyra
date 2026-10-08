local apply = require("../../resolution/apply")
local local_release = require("../../resolution/local_release")

local work_ids: { [number]: string } = {}
local function noop() end
apply.set_provider(setmetatable({
	layer = function(_self: any, db_id: number): any
		return setmetatable({
			set_id = function(_layer: any, id_type: string, id_value: string)
				if id_type == "work_id" then
					work_ids[db_id] = id_value
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
		db_id = 1,
		release_title = "The Album",
		artists = {},
		tracks = {
			{ db_id = 2, track_title = "Song 1", disc = 1, track = 1 },
			{ db_id = 3, track_title = "Song 2", disc = 1, track = 2 },
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
	assert(work_ids[3] == nil, "a recording whose relations weren't fetched keeps its work")
	return work_ids[2]
end

local performance = { ["target-type"] = "work", type = "performance", work = { id = "work" } }
assert(applied_work_id({ performance }) == "work", "a performed work is set")
assert(applied_work_id({}) == "", "relations without a work clear the work")
assert(applied_work_id(nil) == nil, "unfetched relations keep the work")
