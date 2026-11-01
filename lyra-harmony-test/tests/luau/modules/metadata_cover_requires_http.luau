local metadata = require("@lyra/metadata")

-- Cover sync downloads the URLs a cover handler returns, so registering one needs the network.
local provider = metadata.Provider.new("cover-without-http")
local ok, err = pcall(function()
	provider:cover(metadata.EntityType.Release, {}, function()
		return nil
	end)
end)
assert(not ok, "cover registration without harmony.http is refused")
assert(string.find(tostring(err), "provider:cover requires the harmony.http scope", 1, true), tostring(err))
