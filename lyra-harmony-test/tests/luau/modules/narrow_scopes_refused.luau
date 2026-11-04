local covers = require("@lyra/covers")
local genres = require("@lyra/genres")
local labels = require("@lyra/labels")
local libraries = require("@lyra/libraries")
local listens = require("@lyra/listens")
local lyrics = require("@lyra/lyrics")
local metadata = require("@lyra/metadata")
local playback_sessions = require("@lyra/playback_sessions")
local releases = require("@lyra/releases")

-- The test keeps `lyra.covers`, `lyra.genres`, `lyra.labels`, `lyra.libraries`, `lyra.listens`,
-- `lyra.lyrics`, `lyra.metadata`, `lyra.playback_sessions` and `lyra.releases`, but goes without the
-- narrow scopes within them.
local function rejects(f: (...any) -> ...any, message: string, ...: any)
	local ok, err = pcall(f, ...)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

rejects(libraries.sync, "libraries.sync requires the lyra.libraries.manage scope", "id")
rejects(libraries.refresh, "libraries.refresh requires the lyra.libraries.manage scope", "id")
rejects(libraries.cancel_sync, "libraries.cancel_sync requires the lyra.libraries.manage scope", "id")
rejects(metadata.refresh, "metadata.refresh requires the lyra.metadata.refresh scope", "id")
rejects(covers.set_manual, "covers.set_manual requires the lyra.covers.manage scope", "id", "image")
rejects(covers.remove_manual, "covers.remove_manual requires the lyra.covers.manage scope", "id")
rejects(genres.resolve, "genres.resolve requires the lyra.genres.manage scope", { name = "Rock" })
rejects(genres.add_parent, "genres.add_parent requires the lyra.genres.manage scope", "child", "parent")
rejects(labels.resolve, "labels.resolve requires the lyra.labels.manage scope", { name = "Label" })
rejects(releases.replace_chains, "releases.replace_chains requires the lyra.releases.manage scope", "id", {})
rejects(listens.on_update, "listens.on_update requires the lyra.listens.all scope", function() end)
rejects(
	playback_sessions.on_update,
	"playback_sessions.on_update requires the lyra.playback_sessions.all scope",
	function() end
)
rejects(lyrics.remove_all, "lyrics.remove_all requires the lyra.lyrics.all scope", "id")
rejects(
	playback_sessions.list_connections,
	"playback_sessions.list_connections requires the lyra.playback_sessions.all scope"
)
rejects(
	playback_sessions.list_user_playbacks,
	"playback_sessions.list_user_playbacks requires the lyra.playback_sessions.all scope",
	{ all_users = true }
)

-- The rest of each module stays available.
assert(#libraries.list() == 0)
assert(next(libraries.sync_status({ "missing" })) == nil)
assert(next(covers.get({ "missing" })) == nil)
assert(genres.query({}).total == 0)
assert(genres.find_by_name("Rock") == nil)
assert(labels.get_by_id("missing") == nil)
assert(next(releases.get({ "missing" })) == nil)
assert(next(releases.chains({ "missing" })) == nil)
assert(lyrics.parse_lrc("[00:01.00]Hello").plain_text == "Hello")
assert(next(listens.get({})) == nil)
assert(#playback_sessions.list_playbacks() == 0, "the caller's own playbacks need no narrow scope")
rejects(
	playback_sessions.report_session,
	"malformed play_id: ?",
	{ session_key = "idle", track_id = "none", play_id = "?" }
)
