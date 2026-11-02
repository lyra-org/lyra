local fixture = ...
local libraries = require("@lyra/libraries")

-- A listener without the manage_libraries permission sees the library but not its path.
local shown = fixture.libraries.shown
local all = libraries.list()
assert(#all == 1 and all[1].id == shown)
assert(all[1].path == nil, "the path is left out")
assert(libraries.get({ shown })[shown].path == nil)
assert(libraries.by_entity({ shown })[shown][1].path == nil)
