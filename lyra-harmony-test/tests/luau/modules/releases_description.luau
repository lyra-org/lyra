local fixture = ...
local entities = require("@lyra/entities")
local metadata = require("@lyra/metadata")
local releases = require("@lyra/releases")

local ids = fixture.ids

local function description(id: string): string?
	return releases.get({ id })[id].description
end

assert(description(ids.described) == "The album's story", "a release reads its description")
assert(description(ids.bare) == nil, "a release without one reads nil")

local projection = entities.get({ ids.described })[ids.described]
assert(projection.entity.description == "The album's story", "the projection carries the description")

local layer = metadata.Provider.new("description-test"):layer(ids.bare)
layer:set_field("description", "From a provider")
layer:save()
assert(description(ids.bare) == "From a provider", "a provider supplies a release's description")
