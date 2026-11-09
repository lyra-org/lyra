local fixture = ...

local entities = require("@lyra/entities")
local entries = require("@lyra/entries")

local both = fixture.ids.both
local held = entries.by_track({ both })[both]
assert(#held == 1, `the track's visible files: {#held}`)

-- Managing libraries shows paths on disk, and only in the libraries the caller can see.
local files = entities.query_track({ id = both, include = "entries" }).includes.entries
assert(files ~= nil and #files == 1, "the hidden library's file is left out")
assert(files[1].id == held[1].id, "the visible file stays")
assert(files[1].full_path == held[1].full_path and files[1].full_path ~= nil, "a manager reads the path")
