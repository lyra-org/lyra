local fixture = ...
local genres = require("@lyra/genres")
local metadata = require("@lyra/metadata")
local releases = require("@lyra/releases")

local album = fixture.ids.album
local provider = metadata.Provider.new("replace-test")
local fallback = metadata.Provider.new("fallback-test")

local function read(): (string?, string?)
	local names = {}
	for _, genre in genres.by_release({ album })[album] or {} do
		table.insert(names, genre.name)
	end
	return releases.get({ album })[album].description, names[1]
end

local function save(options: metadata.LayerOptions?, fields: { [string]: any })
	local layer = provider:layer(album, options)
	for field, value in fields do
		layer:set_field(field, value)
	end
	layer:save()
end

local lower = fallback:layer(album)
lower:set_field("description", "Fallback")
lower:set_field("genres", { "Jazz" })
lower:save()

save(nil, { description = "Old", genres = { "Rock" } })
local description, genre = read()
assert(description == "Old" and genre == "Rock", `the first save outranks the fallback: {description} / {genre}`)

-- An updating save that sets nothing leaves the layer as stored.
save(nil, {})
description, genre = read()
assert(description == "Old" and genre == "Rock", `an empty updating save keeps the layer: {description} / {genre}`)

-- A replacing save lets what it leaves out fall through.
save({ replace = true }, { description = "New" })
description, genre = read()
assert(description == "New" and genre == "Jazz", `the genre falls through: {description} / {genre}`)

-- One that sets nothing removes the layer.
save({ replace = true }, {})
description, genre = read()
assert(description == "Fallback" and genre == "Jazz", `both fall through: {description} / {genre}`)

local ok, err = pcall(provider.layer, provider, album, { replaces = true })
assert(not ok and string.find(tostring(err), "replaces", 1, true), tostring(err))
