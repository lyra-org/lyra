--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local artists = require("@lyra/artists")
local favorites = require("@lyra/favorites")
local ratings = require("@lyra/ratings")

local cover_art = require("./cover_art")
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
	local user_ratings = ratings.get(ids)
	local cover_art_ids = cover_art.ids(ids)

	local items: { JsonObject } = {}
	for _, summary in summaries do
		local item: JsonObject = {
			id = summary.id,
			name = summary.artist_name,
			coverArt = cover_art_ids[summary.id],
			albumCount = summary.release_count,
			starred = dates.iso8601(starred[summary.id]),
			-- Absent rather than 0 when unrated, as on albums.
			userRating = user_ratings[summary.id],
			sortName = summary.sort_name or "",
		}
		table.insert(items, item)
	end
	return items
end

-- The `Artist` of each artist, in order, as directory-based methods such as `search2` list them.
function module.directories(summaries: { ArtistSummary }): { JsonObject }
	if #summaries == 0 then
		return {}
	end
	local items = module.list(summaries)
	local ids: { string } = {}
	for _, summary in summaries do
		table.insert(ids, summary.id)
	end
	local user_ratings = ratings.get(ids)
	local average_ratings = ratings.average(ids)

	local directories: { JsonObject } = {}
	for _, item in items do
		local directory: JsonObject = {
			id = item.id,
			name = item.name,
			coverArt = item.coverArt,
			starred = item.starred,
			-- Absent rather than 0 when unrated: ratings are 1-5, and 0 is how `setRating` clears one.
			userRating = user_ratings[item.id],
			averageRating = average_ratings[item.id],
		}
		table.insert(directories, directory)
	end
	return directories
end

-- The index letter of each Latin-1 letter from U+00C0 to U+00DF, its accent ignored. Those from
-- U+00E0 are their lowercase forms, 0x20 on.
local LATIN1_LETTERS = "AAAAAAACEEEEIIIIDNOOOOO#OUUUUY#S"

-- An artist is indexed under the first letter of its sort name, else its name, ignoring Latin
-- accents and case, or under `#` without one, as Navidrome's default index groups do.
local function index_name(item: JsonObject): string
	local sort_name: string = item.sortName
	local name: string = if sort_name ~= "" then sort_name else item.name
	local first = string.byte(name, 1)
	if first == nil then
		return "#"
	elseif (first >= 65 and first <= 90) or (first >= 97 and first <= 122) then
		return string.upper(string.char(first))
	end
	-- UTF-8 encodes U+00C0 to U+00FF as 0xC3 followed by 0x80 to 0xBF.
	local second = string.byte(name, 2)
	if first ~= 0xC3 or second == nil or second < 0x80 or second > 0xBF then
		return "#"
	elseif second == 0xBF then
		return "Y" -- ÿ, whose uppercase form is outside Latin-1
	end
	local letter = (second - 0x80) % 0x20 + 1
	return string.sub(LATIN1_LETTERS, letter, letter)
end

-- The `IndexID3` list of these `ArtistID3`s: `#` first, then each letter in order, each holding
-- its artists in the order given.
function module.indexes(items: { JsonObject }): { JsonObject }
	local by_name: { [string]: { JsonObject } } = {}
	local names: { string } = {}
	for _, item in items do
		local name = index_name(item)
		local group = by_name[name]
		if not group then
			group = {}
			by_name[name] = group
			table.insert(names, name)
		end
		table.insert(group, item)
	end
	table.sort(names)
	local indexes: { JsonObject } = {}
	for _, name in names do
		local index: JsonObject = { name = name, artist = by_name[name] }
		table.insert(indexes, index)
	end
	return indexes
end

return module
