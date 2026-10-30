local fixture = ...

local favorites = require("@lyra/favorites")
local tags = require("@lyra/tags")

local ids = fixture.ids
local missing = 999999999
local tag_node = tags.get_for_target(ids.plain)[1].db_id

assert(favorites.add(ids.song), "a visible track is favoritable")
assert(favorites.add(ids.song), "adding again refreshes the favorite")
assert(favorites.add(ids.album), "a release is favoritable")
assert(favorites.add(ids.amy), "an artist is favoritable")
assert(favorites.add(ids.mine), "an owned playlist is favoritable")
assert(not favorites.add(ids.theirs), "another user's private playlist is not favoritable")
assert(favorites.add(ids.shared), "another user's public playlist is favoritable")
assert(not favorites.add(ids.hidden), "a hidden track is not favoritable")
assert(not favorites.add(missing), "a missing target is not favoritable")
assert(not favorites.add(tag_node), "a tag is not favoritable")
for _, id in { 0, -5 } do
	assert(not favorites.add(id), `{id} is not favoritable`)
	assert(not favorites.has(id), `{id} has no favorite`)
	assert(not favorites.remove(id), `{id} has no favorite to remove`)
end

assert(favorites.has(ids.song), "an added favorite is present")
assert(not favorites.has(ids.plain), "an unfavorited track is absent")
assert(favorites.has(ids.shared), "a favorited public playlist is present")
assert(not favorites.has(ids.hidden), "a hidden favorite is absent")
assert(not favorites.has(missing), "a missing target is absent")
assert(not favorites.has(tag_node), "a tag is absent")

local many = favorites.has_many({ ids.song, ids.song, ids.plain, ids.hidden, ids.mine, ids.shared, missing, tag_node })
assert(many[ids.song] == true, "song")
assert(many[ids.mine] == true, "mine")
assert(many[ids.shared] == true, "shared")
for _, id in { ids.plain, ids.hidden, missing, tag_node } do
	assert(many[id] == false, `{id} maps to false`)
end
assert(next(favorites.has_many({})) == nil, "no ids, no states")

local tracks = favorites.list_ids("track")
assert(#tracks == 1 and tracks[1] == ids.song, "list_ids hides the hidden favorite")

assert(favorites.remove(ids.hidden), "a hidden target's favorite can be removed")
assert(favorites.remove(ids.song), "a favorite can be removed")
assert(favorites.remove(ids.song), "removing again still names a favoritable target")
assert(not favorites.has(ids.song), "a removed favorite is gone")
assert(favorites.remove(ids.theirs), "remove has no visibility gate")
assert(not favorites.remove(missing), "a missing target has no favorite")
assert(not favorites.remove(tag_node), "a tag has no favorite")
