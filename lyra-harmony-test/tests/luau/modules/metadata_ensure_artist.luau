local fixture = ...
local metadata = require("@lyra/metadata")

local provider = metadata.Provider.new("ensure-artist-test")
provider:id({ id_type = "artist_id", entity = metadata.EntityType.Artist })

-- A seeded artist goes by its exact name, as a scanned one does.
local found = provider:ensure_artist({ id_type = "artist_id", id_value = "amy", artist_name = "Amy" })
assert(found == fixture.ids.amy, `expected the seeded artist, got {found}`)
