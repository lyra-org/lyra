local fixture = ...

local entries = require("@lyra/entries")
local tracks = require("@lyra/tracks")

local function names(list: { entries.EntryInfo }): string
	local found = {}
	for _, entry in list do
		table.insert(found, entry.name)
	end
	return table.concat(found, ",")
end

-- Browsing walks one level at a time, from the library down to the files.
local library = fixture.ids.music
local top = entries.by_parent({ library })[library]
assert(names(top) == "Amy,song.opus", `library holds {names(top)}`)
local amy = top[1]
assert(amy.kind == "dir" and amy.file_kind == nil and amy.parent_id == library)

local solo = entries.by_parent({ amy.db_id })[amy.db_id]
assert(names(solo) == "Solo", `Amy holds {names(solo)}`)
local folder = solo[1]
local files = entries.by_parent({ folder.db_id })[folder.db_id]
assert(names(files) == "song.opus,song.opus", `Solo holds {names(files)}`)

-- Files lead to their tracks and back, and know the folder holding them.
local held = entries.by_track({ fixture.ids.s1 })[fixture.ids.s1]
assert(#held == 1, `s1 sits in {#held} entries`)
local file = held[1]
assert(file.kind == "file" and file.file_kind == "audio")
assert(file.parent_id == folder.db_id, "s1's file sits in Solo")
assert(file.full_path ~= nil, "a library manager reads paths on disk")

local found = entries.get({ file.db_id, folder.db_id })
assert(found[file.db_id].name == "song.opus" and found[folder.db_id].name == "Solo")

local on_file = tracks.by_entry({ file.db_id, folder.db_id })
assert(#on_file[file.db_id] == 1 and on_file[file.db_id][1].db_id == fixture.ids.s1)
assert(#on_file[folder.db_id] == 0, "a directory holds no track's audio")

-- Neither a file nor a track id has children.
local leaves = entries.by_parent({ file.db_id, fixture.ids.s1 })
assert(next(leaves) == nil)
