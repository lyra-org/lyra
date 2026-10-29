local listens = require("@lyra/listens")

listens.on_update(function(_update: listens.ListenUpdatePayload) end)

local ok = pcall(listens.on_update, "not a function" :: any)
assert(not ok, "on_update takes a function")
