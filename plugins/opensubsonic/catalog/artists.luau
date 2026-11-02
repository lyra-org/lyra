--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists = require("@lyra/artists")
local favorites = require("@lyra/favorites")

local dates = require("../protocol/dates")

type JsonObject = { [string]: any }
type ArtistSummary = artists.ArtistSummary

local module = {}

-- The `ArtistID3` of each artist, in order.
function module.list(summaries: { ArtistSummary }): { JsonObject }
	if #summaries == 0 then
		return {}
	end
	local ids: { string } = {}
	for _, summary in summaries do
		table.insert(ids, summary.id)
	end
	local starred = favorites.get(ids)

	local items: { JsonObject } = {}
	for _, summary in summaries do
		local item: JsonObject = {
			id = summary.id,
			name = summary.artist_name,
			albumCount = summary.release_count,
			starred = dates.iso8601(starred[summary.id]),
			sortName = summary.sort_name or "",
		}
		table.insert(items, item)
	end
	return items
end

return module
