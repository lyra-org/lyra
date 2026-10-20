local fixture = ...
local artists = require("@lyra/artists")
local metadata = require("@lyra/metadata")

local provider = metadata.Provider.new("browsing-test")
local song = fixture.ids.song
local uncredited = fixture.ids.uncredited

local function browsed(): ({ string }, number)
	local page = artists.query({})
	local names = {}
	for _, artist in page.items do
		table.insert(names, artist.artist_name)
	end
	table.sort(names)
	return names, page.total
end

local function save_composers(credits: { metadata.LayerCreditInput })
	local layer = provider:layer(song)
	layer:set_field("track_title", "Song")
	layer:set_credits(metadata.CreditType.Artist, { { artist_id = fixture.ids.credited } })
	layer:set_credits(metadata.CreditType.Composer, credits)
	layer:save()
end

-- Browsing lists only credited artists; lookup by id still finds the others.
local names, total = browsed()
assert(total == 1 and names[1] == "Credited", table.concat(names, ", "))
assert(artists.get({ uncredited })[uncredited] ~= nil, "lookup by id is not browsing")

-- A credit brings the artist into browsing.
save_composers({ { artist_id = uncredited } })
names, total = browsed()
assert(total == 2 and names[1] == "Credited" and names[2] == "Uncredited", table.concat(names, ", "))

-- Losing its last credit takes it out again.
save_composers({})
names, total = browsed()
assert(total == 1 and names[1] == "Credited", table.concat(names, ", "))
