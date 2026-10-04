local metadata = require("@lyra/metadata")
local provider = metadata.Provider.new("credit-test")

local function rejects(credits: any, message: string)
	local ok, err = pcall(function()
		provider:reconcile_artist_credits(1, credits)
	end)
	assert(not ok)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

rejects({ [2] = { artist_id = 1, name = "Artist" } }, "contiguous ordered array")
rejects({ unexpected = { artist_id = 1, name = "Artist" } }, "ordered array")
rejects({ "Artist" }, "entries must be tables")
rejects({ { artist_id = -1, name = "Artist" } }, "positive")
rejects({ { artist_id = 1, name = "" } }, "non-empty string")
rejects({ { artist_id = 1, name = "Artist", join_phrase = false } }, "join_phrase must be a string")
