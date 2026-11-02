local fixture = ...

local covers = require("@lyra/covers")

local ids = fixture.ids
local PNG =
	"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a\x00\x00\x00\x0d\x49\x48\x44\x52\x00\x00\x00\x01\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90\x77\x53\xde\x00\x00\x00\x0c\x49\x44\x41\x54\x78\x9c\x63\x60\x60\x60\x00\x00\x00\x04\x00\x01\xf6\x17\x38\x55\x00\x00\x00\x00\x49\x45\x4e\x44\xae\x42\x60\x82"

-- A manual cover replaces the release's own until it is removed.
assert(covers.get({ ids.a })[ids.a].hash == "a")
assert(covers.set_manual(ids.a, PNG) == true)
local manual = covers.get({ ids.a })[ids.a]
assert(manual.hash ~= "a" and manual.mime_type == "image/png", "the release shows its manual cover")
assert(covers.get({ ids.ta })[ids.ta].hash == manual.hash, "its tracks show it too")
assert(covers.remove_manual(ids.a) == true)
assert(covers.get({ ids.a })[ids.a].hash == "a", "the release's own cover shows again")
assert(covers.remove_manual(ids.a) == false, "no manual cover is left")

-- Artists take one as well.
assert(covers.get({ ids.singer })[ids.singer] == nil)
assert(covers.set_manual(ids.singer, PNG) == true)
assert(covers.get({ ids.singer })[ids.singer] ~= nil)
assert(covers.remove_manual(ids.singer) == true)

-- The caller's own playlist shows its manual cover over the one picked from its releases.
assert(covers.set_manual(ids.mine, PNG) == true)
local own = covers.get({ ids.mine })[ids.mine]
assert(own.hash ~= "a" and own.owner_id == ids.mine, "the cover belongs to the playlist itself")
assert(covers.remove_manual(ids.mine) == true)
local picked = covers.get({ ids.mine })[ids.mine]
assert(picked.hash == "a" and picked.owner_id == ids.a, "the playlist shows its release's cover again")

-- What is missing, hidden or of another type is false.
assert(covers.set_manual("missing-target", PNG) == false)
assert(covers.set_manual(ids.secret, PNG) == false, "a release in a hidden library")
assert(covers.set_manual(ids.private, PNG) == false, "another user's private playlist")
assert(covers.remove_manual(ids.private) == false)
assert(covers.set_manual(ids.ta, PNG) == false, "a track has no cover of its own")

-- Another user's playlist and images that do not decode raise.
assert(not pcall(covers.set_manual, ids.theirs, PNG), "only the owner changes a playlist's cover")
assert(not pcall(covers.remove_manual, ids.theirs))
assert(not pcall(covers.set_manual, ids.a, "not an image"))
assert(not pcall(covers.set_manual, ids.a, 1 :: any))
assert(covers.get({ ids.a })[ids.a].hash == "a", "nothing was stored")
