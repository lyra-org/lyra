-- The test goes without `lyra.bookmarks`, which gates the whole module.
local ok, err = pcall(function()
	return require("@lyra/bookmarks")
end)
assert(not ok, "requiring lyra/bookmarks needs its scope")
assert(string.find(tostring(err), "lyra.bookmarks", 1, true), `error {err}`)
