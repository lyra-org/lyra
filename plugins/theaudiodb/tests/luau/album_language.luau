local fixture = ...
local albums = require("../../resolution/albums")
local locale = require("@lyra/locale")

-- An entity in several libraries takes the language of the first library the server gives
-- for it whose language resolves, as WikiData's artists do, whatever order its releases came in.
local language = albums.library_language(fixture.ids.everywhere)
assert(language and language.iso3 == "fra", `expected French, got {language and language.iso3}`)
local japanese = albums.library_language(fixture.ids.in_japanese)
assert(japanese and japanese.iso3 == "jpn", "a release in one library takes its language")

-- Each translated field is read for its language and recorded by its ISO 639-3 code.
for _, case in
	{
		{ input = "zh", field = "strDescriptionCN", iso3 = "zho" },
		{ input = "he", field = "strDescriptionIL", iso3 = "heb" },
		{ input = "sv", field = "strDescriptionSE", iso3 = "swe" },
		{ input = "ja", field = "strDescriptionJP", iso3 = "jpn" },
		{ input = "nb", field = "strDescriptionNO", iso3 = "nob" },
		{ input = "de", field = "strDescriptionDE", iso3 = "deu" },
	}
do
	local album = { strDescription = "English", [case.field] = `In {case.input}` }
	local description = albums.description(album, locale.language(case.input))
	assert(description, case.input)
	assert(description.text == `In {case.input}`, `{case.input} read {description.text}`)
	assert(description.language == case.iso3, `{case.input} recorded {description.language}`)
end

-- A language TheAudioDB has no field for reads the English text.
local english = albums.description({ strDescription = "English" }, locale.language("ko"))
assert(english and english.text == "English" and english.language == "eng")
