local genres = require("@lyra/genres")

local function resolve_with_id(name: string, value: string): string
	return genres.resolve({
		name = name,
		external_id = { provider_id = "provider", id_type = "genre_id", id_value = value },
	}) :: string
end

-- An ID reached through the genre's name replaces the one it had, which keeps finding the genre.
local rock = resolve_with_id("Rock", "first")
assert(resolve_with_id("Rock", "second") == rock, "the same genre, by name")
assert(resolve_with_id("Anything", "first") == rock, "the replaced id still finds it")
assert(resolve_with_id("Anything else", "second") == rock, "the new id finds it")

-- A name another genre goes by doesn't become an alias, so it keeps leading to that genre.
local pop = genres.resolve({ name = "Pop", aliases = { { name = "Popular" } } })
assert(genres.resolve({ name = "Rock", aliases = { { name = "popular" }, { name = "pop" } } }) == rock)
assert(genres.resolve({ name = "popular" }) == pop, "the alias stays with its genre")
