--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local genres = require("@lyra/genres")

local module = {}

-- The ids of the genres named `name`, compared as search compares names.
function module.ids_named(name: string): { string }
	local ids: { string } = {}
	for _, genre in genres.query({ name = name, available = true }).items do
		table.insert(ids, genre.id)
	end
	return ids
end

return module
