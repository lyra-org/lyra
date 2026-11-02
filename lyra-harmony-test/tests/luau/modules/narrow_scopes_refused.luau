local libraries = require("@lyra/libraries")
local listens = require("@lyra/listens")
local lyrics = require("@lyra/lyrics")
local metadata = require("@lyra/metadata")
local playback_sessions = require("@lyra/playback_sessions")

-- The test keeps `lyra.libraries`, `lyra.listens`, `lyra.lyrics`, `lyra.metadata` and
-- `lyra.playback_sessions`, but goes without the narrow scopes within them.
local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

rejects(libraries.sync, "libraries.sync requires the lyra.libraries.manage scope", "id")
rejects(libraries.refresh, "libraries.refresh requires the lyra.libraries.manage scope", "id")
rejects(libraries.sync_status, "libraries.sync_status requires the lyra.libraries.manage scope", {})
rejects(libraries.cancel_sync, "libraries.cancel_sync requires the lyra.libraries.manage scope", "id")
rejects(metadata.refresh, "metadata.refresh requires the lyra.metadata.refresh scope", "id")
rejects(listens.on_update, "listens.on_update requires the lyra.listens.all scope", function() end)
rejects(
	playback_sessions.on_update,
	"playback_sessions.on_update requires the lyra.playback_sessions.all scope",
	function() end
)
rejects(lyrics.delete_for_track, "lyrics.delete_for_track requires the lyra.lyrics.all scope", 1)
rejects(
	playback_sessions.list_connections,
	"playback_sessions.list_connections requires the lyra.playback_sessions.all scope"
)
rejects(
	playback_sessions.list_playbacks,
	"playback_sessions.list_playbacks requires the lyra.playback_sessions.all scope"
)

-- The rest of each module stays available.
assert(#libraries.list() == 0)
assert(lyrics.parse_lrc("[00:01.00]Hello").plain_text == "Hello")
assert(next(listens.get({})) == nil)
local _, rejection = playback_sessions.report_session({ session_key = "idle", track_id = "none", play_id = "?" })
assert(rejection == "malformed play_id: ?", `rejection {rejection}`)
