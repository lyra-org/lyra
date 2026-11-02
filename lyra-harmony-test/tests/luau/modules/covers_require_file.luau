local fixture = ...

local covers = require("@lyra/covers")

local ids = fixture.ids

-- Reads don't look at cover files unless asked to.
local listed = covers.get({ ids.gone, ids.tgone, ids.kept })
assert(listed[ids.gone].hash == "gone" and listed[ids.tgone].owner_id == ids.gone)

-- `require_file` leaves out a cover whose file is gone and removes its record, so later reads move on.
local served = covers.get({ ids.tgone, ids.kept }, { require_file = true })
assert(served[ids.tgone] == nil, "the track's cover file is gone")
assert(served[ids.kept].hash == "kept", "a cover whose file is there is kept")
local after = covers.get({ ids.gone, ids.tgone })
assert(after[ids.gone] == nil and after[ids.tgone] == nil, "the gone cover is forgotten")

assert(covers.artwork({ ids.kept }, { require_file = true })[ids.kept].hash == "kept")
assert(not pcall(covers.get, { ids.kept }, { require_file = "yes" } :: any))
