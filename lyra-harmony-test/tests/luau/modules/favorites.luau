local fixture = ...

local favorites = require("@lyra/favorites")
local tags = require("@lyra/tags")

local ids = fixture.ids
local missing = "missing-target"
local tag_node = tags.get_for_target(ids.plain)[1].id

assert(favorites.add(ids.song), "a visible track is favoritable")
assert(favorites.add(ids.song), "adding again refreshes the favorite")
assert(favorites.add(ids.album), "a release is favoritable")
assert(favorites.get({ ids.amy })[ids.amy] == 1700000000000, "a favorite keeps when it was first made")
assert(favorites.add(ids.amy), "an artist is favoritable")
assert(favorites.get({ ids.amy })[ids.amy] == 1700000000000, "refreshing keeps the first favorite's time")
assert(favorites.add(ids.mine), "an owned playlist is favoritable")
assert(not favorites.add(ids.theirs), "another user's private playlist is not favoritable")
assert(favorites.add(ids.shared), "another user's public playlist is favoritable")
assert(not favorites.add(ids.hidden), "a hidden track is not favoritable")
assert(not favorites.add(missing), "a missing target is not favoritable")
assert(not favorites.add(tag_node), "a tag is not favoritable")
for _, id in { 1, true } do
	assert(not pcall(favorites.add, id :: any), `{id} is not an id to favorite`)
	assert(not pcall(favorites.get, { id :: any }), `{id} is not an id to look up`)
	assert(not pcall(favorites.remove, id :: any), `{id} is not an id to unfavorite`)
end

local favorited = favorites.get({ ids.song, ids.song, ids.plain, ids.hidden, ids.mine, ids.shared, missing, tag_node })
for _, id in { ids.song, ids.mine, ids.shared } do
	assert(type(favorited[id]) == "number" and favorited[id] > 0, `{id} maps to when it was favorited`)
end
for _, id in { ids.plain, ids.hidden, missing, tag_node } do
	assert(favorited[id] == nil, `{id} is absent`)
end
assert(next(favorites.get({})) == nil, "no ids, no favorites")

local tracks = favorites.list_ids("track")
assert(#tracks == 1 and tracks[1] == ids.song, "list_ids hides the hidden favorite")

-- Removing a target the caller can't see answers as a missing one, whether or not it was a
-- favorite, so removal reveals nothing about what exists.
assert(not favorites.remove(ids.hidden), "a hidden favorite's removal answers as missing")
assert(favorites.remove(ids.song), "a favorite can be removed")
assert(favorites.remove(ids.song), "removing again still names a favoritable target")
assert(favorites.get({ ids.song })[ids.song] == nil, "a removed favorite is gone")
assert(not favorites.remove(ids.theirs), "another user's private playlist answers as missing")
assert(not favorites.remove(missing), "a missing target has no favorite")
assert(not favorites.remove(tag_node), "a tag has no favorite")
