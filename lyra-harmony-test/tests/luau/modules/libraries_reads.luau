local fixture = ...
local libraries = require("@lyra/libraries")

local ids = fixture.ids
local shown = fixture.libraries.shown
local hidden = fixture.libraries.hidden

-- Only the granted library is listed, with its path for a manager.
local all = libraries.list()
assert(#all == 1 and all[1].id == shown, "only the granted library is listed")
assert(all[1].name == "Shown" and all[1].language == "jpn")
assert(all[1].path == "/libraries/shown", `a manager sees the path, got {all[1].path}`)

-- A read by id leaves out what is missing or hidden.
local got = libraries.get({ shown, hidden, "missing" })
assert(got[shown].name == "Shown")
assert(got[hidden] == nil, "a hidden library is absent")
assert(got.missing == nil)
assert(next(libraries.get({})) == nil)

-- Entities map to the libraries holding them; a library holds itself.
local by_entity = libraries.by_entity({ ids.album, ids.song, shown, ids.secret, hidden, "missing" })
assert(#by_entity[ids.album] == 1 and by_entity[ids.album][1].id == shown)
assert(by_entity[ids.song][1].id == shown, "a track is in its release's library")
assert(by_entity[shown][1].id == shown)
assert(by_entity[ids.secret] == nil, "a release in a hidden library is absent")
assert(by_entity[hidden] == nil)
assert(by_entity.missing == nil)
