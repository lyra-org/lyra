local metadata = require("@lyra/metadata")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local serde = require("@harmony/serde")

local response: { [string]: any } = {}
musicbrainz_api.send = function(_options: any): (boolean, any)
	return true, { success = true, status_code = 200, body = serde.json.encode(response), retries = 0 }
end
local apply = require("../../resolution/apply")

local artist_ids: { [string]: string } =
	{ ["actor-a"] = "a11", ["actor-b"] = "a12", ["member"] = "a13", ["band"] = "a14", ["character"] = "a10" }
local saved: { [string]: { [any]: { any } } } = {}
local function noop() end
apply.set_provider(setmetatable({
	ensure_artist = function(_self: any, request: any): string?
		return artist_ids[request.id_value]
	end,
	layer = function(_self: any, node_id: string): any
		local relation_sets: { [any]: { any } } = {}
		return setmetatable({
			set_relations = function(_layer: any, relation_type: any, relations: { any })
				relation_sets[relation_type] = relations
			end,
			save = function()
				saved[node_id] = relation_sets
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

local created_at_ms: { [string]: number } = {}
local reads = 0
apply.set_artist_reader(function(ids: { string }): { [string]: any }
	reads += 1
	local found = {}
	for _, id in ids do
		if created_at_ms[id] ~= nil then
			found[id] = { id = id, artist_name = id, scan_name = id, created_at_ms = created_at_ms[id] }
		end
	end
	return found
end)

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
apply.refresh_artist_by_id("a10", "character")

local sets = saved["a10"]
assert(sets ~= nil, "the artist's layer was saved")
local voice_actors = sets[metadata.ArtistRelationType.VoiceActor]
assert(#voice_actors == 2, "each related artist appears once")
assert(voice_actors[1].artist_id == "a11" and voice_actors[1].attributes == "child, lead")
assert(voice_actors[2].artist_id == "a12" and voice_actors[2].attributes == nil)
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
apply.refresh_artist_by_id("a10", "character")
voice_actors = saved["a10"][metadata.ArtistRelationType.VoiceActor]
assert(#voice_actors == 1 and voice_actors[1].artist_id == "a12", "the self-relation is left out")

response = { id = "character", name = "Character", type = "Character", relations = {} }
apply.refresh_artist_by_id("a10", "character")
assert(#saved["a10"][metadata.ArtistRelationType.VoiceActor] == 0, "dropped relations clear")

-- Related artists go in the order they were created, then by id; one without
-- a creation time goes last. Their times are read once per response.
artist_ids["actor-c"] = "a15"
created_at_ms = { a11 = 30, a12 = 10, a14 = 20 }
reads = 0
response = {
	id = "character",
	name = "Character",
	type = "Character",
	relations = {
		relation("voice actor", "backward", "actor-a"),
		relation("voice actor", "backward", "actor-b"),
		relation("voice actor", "backward", "actor-c"),
		relation("member of band", "backward", "band"),
	},
}
apply.refresh_artist_by_id("a10", "character")
assert(reads == 1, "one read per response")
voice_actors = saved["a10"][metadata.ArtistRelationType.VoiceActor]
assert(#voice_actors == 3)
assert(voice_actors[1].artist_id == "a12", "the earliest created artist goes first")
assert(voice_actors[2].artist_id == "a11")
assert(voice_actors[3].artist_id == "a15", "an artist without a creation time goes last")

created_at_ms = { a11 = 10, a12 = 10 }
apply.refresh_artist_by_id("a10", "character")
voice_actors = saved["a10"][metadata.ArtistRelationType.VoiceActor]
assert(voice_actors[1].artist_id == "a11" and voice_actors[2].artist_id == "a12", "artists created together go by id")
