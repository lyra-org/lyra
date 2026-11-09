local fixture, harness = ...
local artists = require("@lyra/artists")
local errors = require("@lyra/errors")
local metadata = require("@lyra/metadata")
local releases = require("@lyra/releases")

local ids = fixture.ids
local catalog = harness.catalog
local provider = metadata.Provider.new("merge-forwarding-test")
local VOICE_ACTOR = metadata.ArtistRelationType.VoiceActor

local function artist_description(id: string): string?
	return artists.get({ id })[id].description
end

local function save_artist_field(id: string, description: string)
	local layer = provider:layer(id)
	layer:set_field("description", description)
	layer:save()
end

local function voice_actors(id: string): { string }
	local names = {}
	for _, relation in artists.relations({ id })[id] or {} do
		if relation.relation_type == "voice_actor" and relation.direction == "incoming" then
			table.insert(names, relation.artist.artist_name)
		end
	end
	return names
end

local function raises_not_found(fn: () -> ())
	local ok, err = pcall(fn)
	assert(not ok, "the call raises")
	assert(errors.kind(err) == "not_found", tostring(err))
end

-- An artist merged between the context being built and the write: the write lands on the winner.
catalog:merge(ids.target_winner, ids.target_loser)
save_artist_field(ids.target_loser, "Written to the loser")
assert(artist_description(ids.target_winner) == "Written to the loser", "the save reached the winner")

-- Only provider writes forward: everywhere else a merged id is missing.
assert(next(artists.get({ ids.target_loser })) == nil, "a read of a merged id finds nothing")

-- A release the same way.
catalog:merge(ids.release_winner, ids.release_loser)
local release_layer = provider:layer(ids.release_loser)
release_layer:set_field("description", "Written to the release")
release_layer:save()
assert(releases.get({ ids.release_winner })[ids.release_winner].description == "Written to the release")
assert(next(releases.get({ ids.release_loser })) == nil, "a read of a merged release finds nothing")

-- A chain of two merges ends at its last winner.
catalog:merge(ids.chain_second, ids.chain_first)
catalog:merge(ids.chain_last, ids.chain_second)
save_artist_field(ids.chain_first, "Through two merges")
assert(artist_description(ids.chain_last) == "Through two merges")

-- A winner deleted later is gone, and says so.
catalog:merge(ids.gone_winner, ids.gone_loser)
catalog:remove(ids.gone_winner)
raises_not_found(function()
	save_artist_field(ids.gone_loser, "Nowhere to go")
end)
raises_not_found(function()
	provider:mark_unmatched(ids.gone_loser, { "unmatched_id" })
end)

-- A track deleted without a survivor is gone too, for both writes.
local doomed = ids.doomed_track
catalog:remove(doomed)
raises_not_found(function()
	local layer = provider:layer(doomed)
	layer:set_field("track_title", "Doomed")
	layer:save()
end)
raises_not_found(function()
	provider:mark_unmatched(doomed, { "unmatched_id" })
end)

-- A credited artist merged away: credits land on the winner, once.
catalog:merge(ids.credit_winner, ids.credit_loser)
local credit_layer = provider:layer(ids.song)
credit_layer:set_field("track_title", "Song")
credit_layer:set_credits(metadata.CreditType.Artist, {
	{ artist_id = ids.credit_loser },
	{ artist_id = ids.credit_winner },
})
credit_layer:save()
local credited = artists.by_track({ ids.song })[ids.song]
assert(#credited == 1 and credited[1].artist_name == "Credit Winner", "the credits collapse to the winner")

-- A credited artist truly gone stays a plain failure.
local gone_credit = provider:layer(ids.song)
gone_credit:set_credits(metadata.CreditType.Artist, { { artist_id = "no-such-artist" } })
local ok, err = pcall(function()
	gone_credit:save()
end)
assert(not ok, "a gone credited artist raises")
assert(string.find(tostring(err), "artist_id no-such-artist is not an artist", 1, true), tostring(err))
assert(errors.kind(err) == nil, "and not as not_found")

-- A related artist merged away: the relation lands on the winner.
catalog:merge(ids.related_winner, ids.related_loser)
local relation_layer = provider:layer(ids.related)
relation_layer:set_field("artist_name", "Related")
relation_layer:set_relations(VOICE_ACTOR, { { artist_id = ids.related_loser } })
relation_layer:save()
local actors = voice_actors(ids.related)
assert(#actors == 1 and actors[1] == "Related Winner", table.concat(actors, ", "))

-- A stale relation a merge turned into a self-relation is dropped, and the rest of the save kept.
catalog:merge(ids.self_winner, ids.self_loser)
local self_layer = provider:layer(ids.self_loser)
self_layer:set_field("description", "Kept despite the stale relation")
self_layer:set_relations(VOICE_ACTOR, { { artist_id = ids.self_winner } })
self_layer:save()
assert(artist_description(ids.self_winner) == "Kept despite the stale relation")
assert(#voice_actors(ids.self_winner) == 0, "the self-relation was dropped")

-- A self-relation nothing forwarded is still refused.
local explicit = provider:layer(ids.related)
explicit:set_relations(VOICE_ACTOR, { { artist_id = ids.related } })
local explicit_ok, explicit_err = pcall(function()
	explicit:save()
end)
assert(not explicit_ok, "an explicit self-relation is rejected")
assert(string.find(tostring(explicit_err), "metadata layer", 1, true), tostring(explicit_err))

-- So is one naming the layer's own artist, even when a merge took that artist first.
catalog:merge(ids.named_self_winner, ids.named_self_loser)
local named_self = provider:layer(ids.named_self_loser)
named_self:set_relations(VOICE_ACTOR, { { artist_id = ids.named_self_loser } })
local named_ok, named_err = pcall(function()
	named_self:save()
end)
assert(not named_ok, "a self-relation by the merged id is rejected")
assert(string.find(tostring(named_err), "metadata layer", 1, true), tostring(named_err))

-- mark_unmatched on a merged id does nothing, leaving the winner's layers as they were.
local winner_layer = provider:layer(ids.unmatched_release_winner)
winner_layer:set_field("description", "Survives mark_unmatched")
winner_layer:save()
catalog:merge(ids.unmatched_release_winner, ids.unmatched_release_loser)
provider:mark_unmatched(ids.unmatched_release_loser, { "unmatched_id" })
local kept = releases.get({ ids.unmatched_release_winner })[ids.unmatched_release_winner]
assert(kept.description == "Survives mark_unmatched", "the winner's release layer is untouched")
