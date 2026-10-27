local fixture = ...
local catalog = require("../../protocol/items/catalog")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")

-- A genre advertises a primary image only when its image route will serve one.

local listed = {}
for _, item in listing.list_genre_items(options.for_user(options.parse(nil), fixture.users.admin)) do
	listed[item.Name] = item
end

local rock, jazz = listed.Rock, listed.Jazz
assert(rock and jazz, "both genres list")
assert(rock.ImageTags and rock.ImageTags.Primary, "a genre with album art has an image tag")
assert(catalog.resolve_genre_cover(fixture.ids.rock) ~= nil, "and its image is served")
assert(jazz.ImageTags == nil, "a genre without album art has no image tag")
assert(catalog.resolve_genre_cover(fixture.ids.jazz) == nil, "and no image is served")
