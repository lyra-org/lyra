local fixture = ...
local lyrics = require("@lyra/lyrics")

local song = fixture.ids.song

assert(#lyrics.by_track({ song })[song] == 1, "the provider's entry")
assert(lyrics.remove_all(song) == true)
assert(#lyrics.by_track({ song })[song] == 0, "every entry is gone")
assert(lyrics.remove_all(song) == false, "nothing left to remove")
assert(lyrics.remove_all("missing") == false, "a missing track")
assert(lyrics.remove_all(fixture.ids.album) == false, "not a track")
