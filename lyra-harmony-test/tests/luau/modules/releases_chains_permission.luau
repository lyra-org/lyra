local fixture = ...
local releases = require("@lyra/releases")

-- Replacing chains needs ManageMetadata.
local ok, err = pcall(releases.replace_chains, fixture.ids.book, { { fixture.ids.awakening, fixture.ids.blessing } })
assert(not ok and string.find(tostring(err), "ManageMetadata"), tostring(err))
