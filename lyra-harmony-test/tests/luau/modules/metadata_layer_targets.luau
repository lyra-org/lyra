local fixture = ...
local metadata = require("@lyra/metadata")

local ids = fixture.ids
local provider = metadata.Provider.new("layer-target-test")

local function rejects(build: (metadata.Layer) -> (), node_id: string, message: string)
	local ok, err = pcall(function()
		local layer = provider:layer(node_id)
		build(layer)
		layer:save()
	end)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

local function credits(layer: metadata.Layer, artist_id: string)
	layer:set_credits(metadata.CreditType.Artist, { { artist_id = artist_id } })
end

rejects(function(layer)
	layer:set_field("track_title", "Song")
	credits(layer, ids.song)
end, ids.song, `artist_id {ids.song} is not an artist`)

rejects(function(layer)
	layer:set_field("track_title", "Rock")
end, ids.rock, `node_id {ids.rock} is not a release, track or artist`)
rejects(function(layer)
	layer:set_field("track_title", "Missing")
end, "missing-node", "node_id missing-node is not a release, track or artist")

rejects(function(layer)
	layer:set_field("artist_name", "Singer")
	credits(layer, ids.singer)
end, ids.singer, `node_id {ids.singer}`)

rejects(function(layer)
	layer:set_field("track_title", "Song")
	layer:set_relations(metadata.ArtistRelationType.VoiceActor, { { artist_id = ids.singer } })
end, ids.song, `node_id {ids.song}`)

-- A valid layer saves.
local layer = provider:layer(ids.song)
layer:set_field("track_title", "Song")
credits(layer, ids.singer)
layer:save()
