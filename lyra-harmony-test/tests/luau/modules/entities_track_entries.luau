local fixture = ...

local entities = require("@lyra/entities")
local entries = require("@lyra/entries")

local both = fixture.ids.both
local held = entries.by_track({ both })[both]
assert(#held == 1, `the track's visible files: {#held}`)

-- A projection shows only the file the caller's libraries hold, without its path.
local projected = entities.get({ both }, { include = "entries" })[both] :: entities.TrackProjectionInfo
local files = projected.includes.entries
assert(files ~= nil and #files == 1, "the hidden library's file is left out")
assert(files[1].id == held[1].id, "the visible file stays")
assert(files[1].full_path == nil, "a listener reads no paths on disk")
