--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json_helpers = require("./json_helpers")
local libraries = require("@lyra/libraries")
local locale = require("@lyra/locale")
local metadata = require("@lyra/metadata")

type JsonValue = json_helpers.JsonValue
type JsonObject = json_helpers.JsonObject
type JsonArray = json_helpers.JsonArray

local first_non_empty_field = json_helpers.first_non_empty_field
local json_string_field = json_helpers.json_string_field
local trim = json_helpers.trim

local module = {}

local ENGLISH = "eng"

-- The suffixes of TheAudioDB's translated description fields, keyed by ISO
-- 639-1 code. `strDescription` holds the English text.
local DESCRIPTION_SUFFIXES: { [string]: string } = {
	de = "DE",
	es = "ES",
	fr = "FR",
	he = "IL",
	hu = "HU",
	it = "IT",
	ja = "JP",
	nb = "NO",
	nl = "NL",
	nn = "NO",
	no = "NO",
	pl = "PL",
	pt = "PT",
	ru = "RU",
	sv = "SE",
	zh = "CN",
}

export type Description = {
	text: string,
	-- ISO 639-3 code of the text's language.
	language: string,
}

-- The language of the first library holding the entity, in the order the
-- server gives them, whose language resolves.
function module.library_language(entity_id: string): locale.Language?
	local ok, by_entity = pcall(libraries.by_entity, { entity_id })
	if not ok then
		return nil
	end
	for _, library in by_entity[entity_id] or {} do
		local language = locale.language(library.language)
		if language then
			return language
		end
	end
	return nil
end

function module.genre_values(genre_name: string?): JsonArray?
	local genre = trim(genre_name)
	if genre == "" then
		return nil
	end
	return { genre :: JsonValue }
end

-- The album's description in `language`, else in English.
function module.description(album: JsonObject, language: locale.Language?): Description?
	local suffix = if language and language.iso2 then DESCRIPTION_SUFFIXES[language.iso2] else nil
	if language and suffix then
		local text = json_string_field(album, `strDescription{suffix}`)
		if text then
			return { text = text, language = language.iso3 }
		end
	end
	local text = first_non_empty_field(album, { "strDescription", "strDescriptionEN" })
	return if text then { text = text, language = ENGLISH } else nil
end

-- Saves what `album` says of the release as the provider's whole layer, so
-- what TheAudioDB no longer lists falls through to other sources.
function module.save_release_layer(
	provider: metadata.Provider,
	release_id: string,
	album_id: string,
	album: JsonObject,
	language: locale.Language?
)
	local layer = provider:layer(release_id, { replace = true })
	layer:set_id("album_id", album_id)

	local genres = module.genre_values(json_string_field(album, "strGenre"))
	if genres ~= nil then
		layer:set_field("genres", genres)
	end
	local description = module.description(album, language)
	if description then
		layer:set_field("description", description.text)
		layer:set_custom_field("v1", "description_language", description.language)
	else
		layer:clear_custom_fields("v1")
	end

	layer:save()
end

return module
