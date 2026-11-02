local fixture = ...
local metadata = require("@lyra/metadata")

-- Seeing a release is not refreshing it: without sync_metadata the call raises.
local ok, err = pcall(metadata.refresh, fixture.ids.album)
assert(not ok, "the refresh is refused")
assert(string.find(tostring(err), "SyncMetadata permission required", 1, true), tostring(err))
