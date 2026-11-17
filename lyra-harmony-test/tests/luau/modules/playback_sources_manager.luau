local fixture = ...

local playback_sources = require("@lyra/playback_sources")

-- A caller who manages libraries sees where the source's file is.
local source = playback_sources.get({ fixture.ids.song }, { include_entry = true })[fixture.ids.song]
assert(source ~= nil, "song has a playback source")
local full_path = source.entry.full_path
assert(full_path ~= nil and string.sub(full_path, -#"song.opus") == "song.opus", `full path {full_path}`)
