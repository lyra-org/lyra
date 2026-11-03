local genres = require("@lyra/genres")
local labels = require("@lyra/labels")

-- A user with manage_libraries resolves a genre or label, creating it once, and links genres.
local id = genres.resolve({ name = "Shoegaze" })
assert(id ~= nil)
assert(genres.resolve({ name = "shoegaze" }) == id, "the same genre, ignoring case")
local found = genres.find_by_name("Shoegaze")
assert(found ~= nil and found.id == id)

local parent = genres.resolve({ name = "Rock" }) :: string
genres.add_parent(id :: string, parent)

local label = labels.resolve({ name = "Creation" })
assert(label ~= nil)
assert(labels.resolve({ name = "Creation" }) == label, "the same label")
assert((labels.get_by_id(label :: string) :: any).name == "Creation")
