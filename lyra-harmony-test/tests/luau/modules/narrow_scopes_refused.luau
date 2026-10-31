local listens = require("@lyra/listens")
local lyrics = require("@lyra/lyrics")
local playback_sessions = require("@lyra/playback_sessions")

-- The test keeps `lyra.listens`, `lyra.lyrics` and `lyra.playback_sessions`, but goes without the
-- narrow scopes within them.
local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

rejects(listens.on_update, "listens.on_update requires the lyra.listens.all scope", function() end)
rejects(
	playback_sessions.on_update,
	"playback_sessions.on_update requires the lyra.playback_sessions.all scope",
	function() end
)
rejects(lyrics.delete_for_track, "lyrics.delete_for_track requires the lyra.lyrics.all scope", 1)

-- The rest of each module stays available.
assert(lyrics.parse_lrc("[00:01.00]Hello").plain_text == "Hello")
assert(next(listens.get({})) == nil)
assert(#playback_sessions.list_connections() == 0)
