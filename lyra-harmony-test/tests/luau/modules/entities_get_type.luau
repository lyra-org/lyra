local fixture = ...
local entities = require("@lyra/entities")

local ids = fixture.ids

assert(entities.get_type(ids.song) == "Track", "a visible track")
assert(entities.get_type(ids.album) == "Release", "a visible release")
assert(entities.get_type(ids.rock) == "Genre", "a genre on a visible release")

-- What the caller can't see reads as an id naming nothing.
assert(entities.get_type(ids.hidden) == nil, "a track in an ungranted library")
assert(entities.get_type(ids.secret) == nil, "a release in an ungranted library")
assert(entities.get_type("missing-entity") == nil, "an id naming nothing")
