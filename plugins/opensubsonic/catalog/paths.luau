--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local entries = require("@lyra/entries")

type EntryInfo = entries.EntryInfo

-- Deeper directories than this are left out of a path.
local MAX_DEPTH = 64

local module = {}

-- Each file's path within its library, keyed by entry id, read a directory level at a time.
function module.relative(files: { EntryInfo }): { [string]: string }
	local names: { [string]: string } = {}
	local parents: { [string]: string } = {}
	local roots: { [string]: boolean } = {}
	local pending: { string } = {}
	for _, file in files do
		names[file.id] = file.name
		parents[file.id] = file.parent_id
		table.insert(pending, file.parent_id)
	end

	for _ = 1, MAX_DEPTH do
		local batch: { string } = {}
		local queued: { [string]: boolean } = {}
		for _, id in pending do
			if not names[id] and not roots[id] and not queued[id] then
				queued[id] = true
				table.insert(batch, id)
			end
		end
		if #batch == 0 then
			break
		end
		local found = entries.get(batch)
		pending = {}
		for _, id in batch do
			local directory = found[id]
			if directory then
				names[id] = directory.name
				parents[id] = directory.parent_id
				table.insert(pending, directory.parent_id)
			else
				-- A library, which entries.get doesn't return.
				roots[id] = true
			end
		end
	end

	local paths: { [string]: string } = {}
	for _, file in files do
		local segments: { string } = {}
		local id: string? = file.id
		while id and names[id] and #segments <= MAX_DEPTH do
			table.insert(segments, 1, names[id])
			id = parents[id]
		end
		paths[file.id] = table.concat(segments, "/")
	end
	return paths
end

return module
