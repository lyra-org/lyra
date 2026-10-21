local metadata = require("@lyra/metadata")
local musicbrainz_api = require("../../resolution/musicbrainz_api")

local response: { [string]: any } = {}
musicbrainz_api.mb_get = function(_endpoint: string): (any, number)
	return response, 200
end
local apply = require("../../resolution/apply")

local artist_ids: { [string]: number } =
	{ ["actor-a"] = 11, ["actor-b"] = 12, ["member"] = 13, ["band"] = 14, ["character"] = 10 }
local saved: { [number]: { [any]: { any } } } = {}
local function noop() end
apply.set_provider(setmetatable({
	ensure_artist = function(_self: any, request: any): number?
		return artist_ids[request.id_value]
	end,
	layer = function(_self: any, db_id: number): any
		local relation_sets: { [any]: { any } } = {}
		return setmetatable({
			set_relations = function(_layer: any, relation_type: any, relations: { any })
				relation_sets[relation_type] = relations
			end,
			save = function()
				saved[db_id] = relation_sets
			end,
		}, {
			__index = function()
				return noop
			end,
		})
	end,
}, {
	__index = function()
		return noop
	end,
}) :: any)

local function relation(kind: string, direction: string, id: string, attributes: { string }?): any
	return {
		["target-type"] = "artist",
		type = kind,
		direction = direction,
		attributes = attributes,
		artist = { id = id, name = id },
	}
end

response = {
	id = "character",
	name = "Character",
	type = "Character",
	relations = {
		relation("voice actor", "backward", "actor-a", { "lead" }),
		relation("voice actor", "backward", "actor-b"),
		relation("voice actor", "backward", "actor-a", { "lead", "child" }),
		relation("member of band", "forward", "band"),
	},
}
apply.refresh_artist_by_id(10, "character")

local sets = saved[10]
assert(sets ~= nil, "the artist's layer was saved")
local voice_actors = sets[metadata.ArtistRelationType.VoiceActor]
assert(#voice_actors == 2, "each related artist appears once")
assert(voice_actors[1].artist_id == 11 and voice_actors[1].attributes == "child, lead")
assert(voice_actors[2].artist_id == 12 and voice_actors[2].attributes == nil)
assert(
	#sets[metadata.ArtistRelationType.MemberOf] == 0,
	"a forward relation belongs to the other artist's set, so this one clears"
)

-- A relation that resolves to the artist itself is left out rather than
-- failing the save.
response = {
	id = "character",
	name = "Character",
	type = "Character",
	relations = {
		relation("voice actor", "backward", "character"),
		relation("voice actor", "backward", "actor-b"),
	},
}
apply.refresh_artist_by_id(10, "character")
voice_actors = saved[10][metadata.ArtistRelationType.VoiceActor]
assert(#voice_actors == 1 and voice_actors[1].artist_id == 12, "the self-relation is left out")

response = { id = "character", name = "Character", type = "Character", relations = {} }
apply.refresh_artist_by_id(10, "character")
assert(#saved[10][metadata.ArtistRelationType.VoiceActor] == 0, "dropped relations clear")
