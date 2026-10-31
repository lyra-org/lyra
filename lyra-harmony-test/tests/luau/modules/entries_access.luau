local fixture = ...

local entries = require("@lyra/entries")
local tracks = require("@lyra/tracks")

-- A listener browses only the libraries granted to them, and reads no paths on disk.
local shown = entries.by_parent({ fixture.ids.shown, fixture.ids.hidden })
assert(shown[fixture.ids.hidden] == nil, "an ungranted library is not browsable")
local file = shown[fixture.ids.shown][1]
assert(file.name == "song.opus" and file.full_path == nil)

local held = entries.by_track({ fixture.ids.visible, fixture.ids.invisible })
assert(held[fixture.ids.invisible] == nil, "an unseen track's entries are hidden")
assert(held[fixture.ids.visible][1].id == file.id)

-- The hidden library's file is unreachable by id too.
local hidden = entries.by_parent({ fixture.ids.hidden })
assert(next(hidden) == nil)
assert(next(entries.get({ fixture.ids.invisible })) == nil, "a track id is not an entry")
assert(tracks.by_entry({ file.id })[file.id][1].id == fixture.ids.visible)
