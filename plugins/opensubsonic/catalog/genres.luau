--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local genres = require("@lyra/genres")

local MATCH_LIMIT = 100

local module = {}

-- The ids of the genres named `name`, ignoring case.
function module.ids_named(name: string): { string }
	local wanted = string.lower(name)
	local ids: { string } = {}
	for _, genre in genres.query({ search = name, limit = MATCH_LIMIT }).items do
		if string.lower(genre.name) == wanted then
			table.insert(ids, genre.id)
		end
	end
	return ids
end

return module
