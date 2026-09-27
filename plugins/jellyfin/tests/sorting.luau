local enrichment = require("../protocol/items/enrichment")
local sorting = require("../protocol/items/sorting")

local function ids(items: { any }): string
	local values: { string } = {}
	for _, item in items do
		table.insert(values, item.Id)
	end
	return table.concat(values, ",")
end

local function track(id: string, fields: { [string]: any }): any
	local item: { [string]: any } = { Id = id, Name = id, SortName = id, _type = "track", _release_keys_loaded = true }
	for key, value in fields do
		item[key] = value
	end
	return item
end

local played = {
	track("a", { _play_count = 2, _last_played = 100 }),
	track("b", { _play_count = 0 }),
	track("c", { _play_count = 5, _last_played = 50 }),
	track("d", { _play_count = 2, _last_played = 300 }),
}

sorting.apply(played, { sort_by = { "PlayCount", "SortName" }, sort_order = "Descending" })
assert(ids(played) == "c,d,a,b", ids(played))

sorting.apply(played, { sort_by = { "DatePlayed", "SortName" }, sort_order = "Descending" })
assert(ids(played) == "d,a,c,b", ids(played))

local albums = {
	track("x", { Album = "Beta", SortName = "0001 - x" }),
	track("y", { Album = "alpha", SortName = "0002 - y" }),
	track("z", { Album = "Beta", SortName = "0000 - z" }),
	track("w", {}),
}
sorting.apply(albums, { sort_by = { "Album", "SortName" } })
assert(ids(albums) == "y,z,x,w", ids(albums))

local album_tracks = {
	track("third", { SortName = "0002 - 0001 - Third" }),
	track("second", { SortName = "0001 - 0002 - Second" }),
	track("first", { SortName = "0001 - 0001 - First" }),
}
sorting.apply(album_tracks, { sort_by = { "SortName" } })
assert(ids(album_tracks) == "first,second,third", ids(album_tracks))

local years = {
	track("p", { ProductionYear = 2001, ChildCount = 3, RunTimeTicks = 30 }),
	track("q", { ProductionYear = 1999, ChildCount = 1, RunTimeTicks = 10 }),
	track("r", { ChildCount = 2, RunTimeTicks = 20 }),
}
sorting.apply(years, { sort_by = { "ProductionYear", "PremiereDate", "SortName" }, sort_order = "Descending" })
assert(ids(years) == "p,q,r", ids(years))
sorting.apply(years, { sort_by = { "ChildCount" } })
assert(ids(years) == "q,r,p", ids(years))
sorting.apply(years, { sort_by = { "Runtime" }, sort_order = "Descending" })
assert(ids(years) == "p,r,q", ids(years))
sorting.apply(years, { sort_by = { "CriticRating", "SortName" } })
assert(ids(years) == "p,q,r", ids(years))

assert(ids(enrichment.filter_years(years, { 1999, 2001 })) == "p,q")
assert(ids(enrichment.filter_years(years, {})) == "p,q,r")
assert(ids(enrichment.filter_played(played, true)) == "d,a,c")
assert(ids(enrichment.filter_played(played, false)) == "b")
assert(ids(enrichment.filter_played(played, nil)) == "d,a,c,b")

assert(enrichment.release_year("2004-05-06") == 2004)
assert(enrichment.release_date_to_premiere_date("2004") == "2004-01-01T00:00:00.0000000Z")
assert(enrichment.sum_runtime_ticks({}) == 0)
