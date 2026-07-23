--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local covers = require("@lyra/covers")

local identifiers = require("./identifiers")
local requests = require("./requests")

type RawCoverInfo = covers.CoverInfo

export type ResolvedCoverInfo = {
	path: string,
	mime_type: string,
	hash: string,
	blurhash: string?,
	release_id: string?,
}

local module = {}

function module.normalize_cover_info(raw_cover: RawCoverInfo?): ResolvedCoverInfo?
	if type(raw_cover) ~= "table" then
		return nil
	end

	local path = requests.trim(raw_cover.path)
	local hash = requests.trim(raw_cover.hash)
	if not path or not hash then
		return nil
	end

	return {
		path = path,
		mime_type = requests.trim(raw_cover.mime_type) or "application/octet-stream",
		hash = hash,
		blurhash = requests.trim(raw_cover.blurhash),
		release_id = identifiers.encode_jellyfin_item_id(raw_cover.release_id),
	}
end

function module.get_cover_info(item_id: number?): ResolvedCoverInfo?
	if item_id == nil or item_id <= 0 then
		return nil
	end
	local ok, raw_cover = pcall(covers.get, item_id)
	if not ok then
		return nil
	end
	return module.normalize_cover_info(raw_cover)
end

return module
