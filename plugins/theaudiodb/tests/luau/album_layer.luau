local albums = require("../../resolution/albums")

type Saved = { options: any, ids: { [string]: string }, fields: { [string]: any } }

local saves: { Saved } = {}
local provider = {
	layer = function(_self: any, _node_id: string, options: any): any
		local saved: Saved = { options = options, ids = {}, fields = {} }
		return {
			set_id = function(_layer: any, id_type: string, value: string)
				saved.ids[id_type] = value
			end,
			set_field = function(_layer: any, field: string, value: any)
				saved.fields[field] = value
			end,
			save = function()
				table.insert(saves, saved)
			end,
		}
	end,
}

albums.save_release_layer(provider :: any, "release", "1", {
	strDescription = " TheAudioDB story ",
	strGenre = "Rock",
	strReview = "A review",
})
local first = saves[1]
assert(first.options and first.options.replace == true, "the layer replaces what the provider stored")
assert(first.ids.album_id == "1")
assert(first.fields.description == "TheAudioDB story", tostring(first.fields.description))
assert(first.fields.genres[1] == "Rock" and #first.fields.genres == 1)

-- An album that no longer lists either leaves both unset, so they fall through.
albums.save_release_layer(provider :: any, "release", "1", { strDescription = "  ", strReview = "A review" })
local second = saves[2]
assert(second.options.replace == true)
assert(second.fields.description == nil, "a review is not a description")
assert(second.fields.genres == nil)

-- Without the English description, the EN-suffixed one stands in.
albums.save_release_layer(provider :: any, "release", "1", { strDescriptionEN = "English story" })
assert(saves[3].fields.description == "English story")
