--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json_helpers = require("./json_helpers")
local metadata = require("@lyra/metadata")

type JsonValue = json_helpers.JsonValue
type JsonObject = json_helpers.JsonObject
type JsonArray = json_helpers.JsonArray

local first_non_empty_field = json_helpers.first_non_empty_field
local json_string_field = json_helpers.json_string_field
local trim = json_helpers.trim

local module = {}

function module.genre_values(genre_name: string?): JsonArray?
	local genre = trim(genre_name)
	if genre == "" then
		return nil
	end
	return { genre :: JsonValue }
end

function module.description(album: JsonObject): string?
	return first_non_empty_field(album, { "strDescription", "strDescriptionEN" })
end

-- Saves what `album` says of the release as the provider's whole layer, so
-- what TheAudioDB no longer lists falls through to other sources.
function module.save_release_layer(provider: metadata.Provider, release_id: string, album_id: string, album: JsonObject)
	local layer = provider:layer(release_id, { replace = true })
	layer:set_id("album_id", album_id)

	local genres = module.genre_values(json_string_field(album, "strGenre"))
	if genres ~= nil then
		layer:set_field("genres", genres)
	end
	local description = module.description(album)
	if description ~= nil then
		layer:set_field("description", description)
	end

	layer:save()
end

return module
