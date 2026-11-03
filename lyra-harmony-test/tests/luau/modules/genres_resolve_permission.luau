local fixture = ...
local genres = require("@lyra/genres")
local labels = require("@lyra/labels")

local ids = fixture.ids

-- Changing the shared genres and labels needs manage_libraries: resolving answers nil, as for a
-- target the caller can't reach, and linking, which answers nothing, raises.
assert(genres.resolve({ name = "Shoegaze" }) == nil)
assert(genres.find_by_name("Shoegaze") == nil, "nothing was created")
assert(labels.resolve({ name = "Creation" }) == nil)
local ok, err = pcall(genres.add_parent, ids.punk, ids.rock)
assert(not ok and string.find(tostring(err), "ManageLibraries permission required", 1, true), tostring(err))
