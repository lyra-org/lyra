local images = require("@lyra/images")

-- Compose fails after waiting on its blocking work; pcall still catches it.
local ok, err = pcall(images.compose, { sources = { "/" } })
assert(not ok, "composing a source outside the media roots succeeded")
assert(
	string.find(tostring(err), "module 'lyra/images' function 'compose'", 1, true)
		and string.find(tostring(err), "no source is inside the library and cover roots", 1, true),
	tostring(err)
)
