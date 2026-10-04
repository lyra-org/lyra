local metadata = require("@lyra/metadata")
local provider = metadata.Provider.new("id-link-template-test")

provider:id({ id_type = "accepted_id", entity = metadata.EntityType.Release }, "https://example.test/release/{id}")

local ok, err = pcall(function()
	provider:id({ id_type = "rejected_id", entity = metadata.EntityType.Release }, "/release/{id}")
end)
assert(not ok)
assert(string.find(tostring(err), "provider:id URL template must be an absolute http(s) URL", 1, true), tostring(err))
