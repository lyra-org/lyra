local fixture = ...
local albums = require("../../resolution/albums")
local locale = require("@lyra/locale")

type Saved = {
	options: any,
	ids: { [string]: string },
	fields: { [string]: any },
	custom: { [string]: any },
	cleared: boolean,
}

local saves: { Saved } = {}
local provider = {
	layer = function(_self: any, _node_id: string, options: any): any
		local saved: Saved = { options = options, ids = {}, fields = {}, custom = {}, cleared = false }
		return {
			set_id = function(_layer: any, id_type: string, value: string)
				saved.ids[id_type] = value
			end,
			set_field = function(_layer: any, field: string, value: any)
				saved.fields[field] = value
			end,
			set_custom_field = function(_layer: any, version: string, name: string, value: any)
				assert(version == "v1")
				saved.custom[name] = value
			end,
			clear_custom_fields = function(_layer: any, version: string)
				assert(version == "v1")
				saved.cleared = true
			end,
			save = function()
				table.insert(saves, saved)
			end,
		}
	end,
}

local function save(album: { [string]: any }, language: locale.Language?): Saved
	albums.save_release_layer(provider :: any, "release", "1", album, language)
	return saves[#saves]
end

local french = locale.language("fra")
local japanese = locale.language("jpn")

-- A release takes the language of its library; one without a language lends none.
local language = albums.library_language(fixture.ids.in_french)
assert(language and language.iso3 == "fra", `expected French, got {language and language.iso3}`)
assert(albums.library_language(fixture.ids.in_plain) == nil)

local saved = save({
	strDescription = " English story ",
	strDescriptionFR = "Histoire",
	strGenre = "Rock",
	strReview = "A review",
}, french)
assert(saved.options and saved.options.replace == true, "the layer replaces what the provider stored")
assert(saved.ids.album_id == "1")
assert(saved.fields.description == "Histoire", tostring(saved.fields.description))
assert(saved.custom.description_language == "fra")
assert(saved.fields.genres[1] == "Rock" and #saved.fields.genres == 1)

-- Without text in the library's language, or without a language, the English text stands in.
saved = save({ strDescription = "English story", strDescriptionFR = " " }, french)
assert(saved.fields.description == "English story" and saved.custom.description_language == "eng")
saved = save({ strDescription = "English story", strDescriptionJP = "物語" }, nil)
assert(saved.fields.description == "English story" and saved.custom.description_language == "eng")
saved = save({ strDescriptionEN = "English story" }, japanese)
assert(saved.fields.description == "English story", "the EN-suffixed field is English too")
saved = save({ strDescription = "English story", strDescriptionJP = "物語" }, japanese)
assert(saved.fields.description == "物語" and saved.custom.description_language == "jpn")

-- An album that no longer lists either leaves both unset, so they fall through.
saved = save({ strDescription = "  ", strReview = "A review" }, french)
assert(saved.options.replace == true)
assert(saved.fields.description == nil, "a review is not a description")
assert(saved.fields.genres == nil)
assert(saved.cleared and next(saved.custom) == nil, "the description's language goes with it")
