local fixture = ...
local artists = require("@lyra/artists")
local metadata = require("@lyra/metadata")

local provider = metadata.Provider.new("relation-test")
local character = fixture.ids.character
local first = fixture.ids.first
local second = fixture.ids.second

-- The character's voice actors by name, with their attributes.
local function voice_actors(): { [string]: string }
	local found = {}
	for _, relation in artists.relations({ character })[character] or {} do
		if relation.relation_type == "voice_actor" and relation.direction == "incoming" then
			found[relation.artist.artist_name] = relation.attributes or ""
		end
	end
	return found
end

local function save_relations(relations: { metadata.LayerRelationInput }?)
	local layer = provider:layer(character)
	layer:set_field("artist_name", "Character")
	if relations then
		layer:set_relations(metadata.ArtistRelationType.VoiceActor, relations)
	end
	layer:save()
end

-- A set replaces the type's relations into the layer's artist.
save_relations({ { artist_id = first, attributes = " lead " } })
local found = voice_actors()
assert(found["First Actor"] == "lead", "the relation is applied with trimmed attributes")

save_relations({ { artist_id = second } })
found = voice_actors()
assert(found["First Actor"] == nil, "a replaced relation is removed")
assert(found["Second Actor"] == "", "the new relation is applied")

-- A save that does not set the type keeps it.
save_relations(nil)
assert(voice_actors()["Second Actor"] == "", "an unset type keeps its relations")

-- An empty set clears the type.
save_relations({})
assert(next(voice_actors()) == nil, "an empty set clears the type")

local function rejects(relations: any, message: string)
	local ok, err = pcall(function()
		provider:layer(character):set_relations(metadata.ArtistRelationType.VoiceActor, relations)
	end)
	assert(not ok)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

rejects({ { artist_id = -1 } }, "positive")
rejects({ { id = first } }, "artist_id")
