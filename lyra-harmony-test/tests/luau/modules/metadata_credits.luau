local fixture = ...
local artists = require("@lyra/artists")
local metadata = require("@lyra/metadata")

local provider = metadata.Provider.new("credit-test")
local song = fixture.ids.song

local function credited(): { string }
	local names = {}
	for _, artist in artists.by_track({ song })[song] or {} do
		table.insert(names, artist.artist_name)
	end
	return names
end

local function save_credits(credits: { metadata.LayerCreditInput }?)
	local layer = provider:layer(song)
	layer:set_field("track_title", "Song")
	if credits then
		layer:set_credits(metadata.CreditType.Artist, credits)
	end
	layer:save()
end

-- A set replaces the role's credits, in order.
save_credits({ { artist_id = fixture.ids.second }, { artist_id = fixture.ids.first } })
local names = credited()
assert(#names == 2 and names[1] == "Second" and names[2] == "First", table.concat(names, ", "))

-- A save that does not set the role keeps it.
save_credits(nil)
assert(#credited() == 2, "an unset role keeps its credits")

-- An empty set clears the role.
save_credits({})
assert(#credited() == 0, "an empty set clears the role")

local function rejects(credits: any, message: string)
	local ok, err = pcall(function()
		provider:layer(song):set_credits(metadata.CreditType.Artist, credits)
	end)
	assert(not ok)
	assert(string.find(tostring(err), message, 1, true), tostring(err))
end

rejects({ { artist_id = 1 } }, "expected a string")
rejects({ { name = "Artist" } }, "artist_id")
rejects({ { artist_id = fixture.ids.first, join_phrase = " & " } }, "join_phrase")
