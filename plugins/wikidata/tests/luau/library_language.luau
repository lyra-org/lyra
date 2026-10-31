local fixture = ...

local artists = require("../../resolution/artists")

-- An artist in several libraries takes the language of the library the server picks for it,
-- the first created, whatever order its releases were added in.
local language = artists.library_language(fixture.ids.both)
assert(language ~= nil, "an artist in libraries with languages has one")
assert(language.site == "fr" and language.iso3 == "fra", `expected French, got {language.iso3}`)

assert(artists.library_language(fixture.ids.unlabelled) == nil, "a library without a language lends none")
