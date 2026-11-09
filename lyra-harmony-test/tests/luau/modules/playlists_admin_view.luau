local fixture = ...
local hidden: string = fixture.ids.hidden
local shared: string = fixture.ids.shared

local covers = require("@lyra/covers")
local entities = require("@lyra/entities")
local favorites = require("@lyra/favorites")
local mix = require("@lyra/mix")
local tags = require("@lyra/tags")

-- Every module shows an admin another user's private playlist as missing, as `playlists` does.
assert(entities.get_type(hidden) == nil, "get_type")
assert(entities.get_type(shared) == "Playlist")
assert(covers.get({ hidden })[hidden] == nil, "covers")
assert(covers.remove_manual(hidden) == false, "covers.remove_manual")
assert(favorites.add(hidden) == false, "favorites.add")
assert(favorites.remove(hidden) == false, "favorites.remove")
assert(favorites.add(shared) == true)
assert(favorites.get({ hidden, shared })[hidden] == nil)
assert(favorites.get({ hidden, shared })[shared] ~= nil)
assert(tags.add(hidden, "Mine", "red") == nil, "tags.add")
assert(tags.add(shared, "Mine", "red") ~= nil)
assert(tags.by_target({ hidden })[hidden] == nil, "tags.by_target")
assert(mix.blend({ seeds = { { id = hidden, exclude = false } } }) == nil, "a private playlist seed is missing")
assert(mix.blend({ seeds = { { id = shared, exclude = false } } }) ~= nil, "a public one seeds")
