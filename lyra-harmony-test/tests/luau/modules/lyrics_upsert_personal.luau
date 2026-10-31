local fixture = ...
local lyrics = require("@lyra/lyrics")

local ids = fixture.ids
local upload = { content_type = "text/plain", body = "Words" }

-- Only a track takes lyrics; anything else reads as a missing track.
for _, id in { ids.album, "missing-track" } do
	local ok, err = pcall(lyrics.upsert_personal, id, upload)
	assert(not ok, `{id} is rejected`)
	assert(string.find(tostring(err), `track not found: {id}`, 1, true), tostring(err))
end

assert(lyrics.upsert_personal(ids.song, upload).plain_text == "Words", "a track takes lyrics")
