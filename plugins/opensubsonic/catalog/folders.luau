--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local libraries = require("@lyra/libraries")

local request = require("../protocol/request")

type ApiQueryParams = api.ApiQueryParams

export type MusicFolder = { id: number, name: string }

local FNV_OFFSET_BASIS = 2166136261

local module = {}

-- Music folder ids are integers, so each library's is a positive 31-bit FNV-1a hash of its id:
-- stable for as long as the library exists, with nothing stored.
function module.id(library_id: string): number
	local hash = FNV_OFFSET_BASIS
	for index = 1, #library_id do
		hash = bit32.bxor(hash, string.byte(library_id, index))
		-- Multiplies by the FNV prime, 2^24 + 403, without leaving the exact integer range.
		hash = bit32.band(bit32.lshift(hash, 24) + hash * 403, 0xFFFFFFFF)
	end
	local id = bit32.band(hash, 0x7FFFFFFF)
	return if id == 0 then 1 else id
end

function module.list(): { MusicFolder }
	local folders: { MusicFolder } = {}
	for _, library in libraries.list() do
		table.insert(folders, { id = module.id(library.id), name = library.name })
	end
	return folders
end

-- The library `musicFolderId` names, nil without one. Fails with 70 for a folder the caller can't
-- see.
function module.library_id(params: ApiQueryParams): string?
	local folder_id = request.int(params, "musicFolderId")
	if folder_id == nil then
		return nil
	end
	for _, library in libraries.list() do
		if module.id(library.id) == folder_id then
			return library.id
		end
	end
	return request.not_found("Music folder")
end

return module
