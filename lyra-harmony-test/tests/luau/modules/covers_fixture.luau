local fixture = ...
local covers = require("@lyra/covers")

local ids = fixture.ids
local found = covers.get({ ids.singer, ids.mix, ids.a })

-- An artist's and a playlist's fixture covers are their own, each hashed by its key.
local singer = found[ids.singer]
assert(singer.owner_id == ids.singer and singer.mime_type == "image/png", singer.owner_id)
assert(singer.hash == "singer", singer.hash)
local mix = found[ids.mix]
assert(mix.owner_id == ids.mix and mix.hash == "mix", "the playlist's own cover, not its release's")
assert(found[ids.a].hash == "a")
