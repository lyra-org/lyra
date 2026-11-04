local fixture, harness = ...
local call = require("./_call")(harness)
local serde = require("@harmony/serde")

local ids = fixture.ids
local key = fixture.api_keys.listener_key
local neighbour = fixture.api_keys.neighbour_key

local function now_playing(api_key: string?): { any }
	return call.ok("getNowPlaying", api_key or key).nowPlaying.entry or {}
end

local function report(track: string, state: string, position_ms: number, extra: { [string]: string }?)
	local params: { [string]: string } = {
		mediaId = ids[track],
		mediaType = "song",
		positionMs = tostring(position_ms),
		state = state,
		c = "Phone",
	}
	for name, value in extra or {} do
		params[name] = value
	end
	call.ok("reportPlayback", key, params)
end

local function play_count(track: string): number
	return call.ok("getSong", key, { id = ids[track] }).song.playCount
end

-- Plays the song from its start for longer than a listen needs, then stops it.
local function play_through(track: string, extra: { [string]: string }?)
	report(track, "playing", 0, extra)
	harness.task:wait(3.2)
	report(track, "stopped", 3200, extra)
end

local extensions = call.ok("getOpenSubsonicExtensions", key).openSubsonicExtensions
local offered = false
for _, extension in extensions do
	offered = offered or extension.name == "playbackReport"
end
assert(offered, "the playbackReport extension is offered")

assert(#now_playing() == 0, "nothing plays yet")

-- A starting player shows with its position held, and plays as one client of the caller's.
report("one", "starting", 0)
local starting = now_playing()
assert(#starting == 1 and starting[1].id == ids.one, "the starting song")
local entry = starting[1]
assert(entry.state == "starting" and entry.positionMs == 0, `starting at {entry.positionMs}`)
assert(entry.playerName == "Phone" and entry.minutesAgo == 0, `player {entry.playerName}`)
assert(type(entry.username) == "string" and entry.username ~= "", "the username")
assert(
	type(entry.playerId) == "number" and entry.playerId >= 0 and entry.playerId < 2 ^ 31,
	`player id {entry.playerId}`
)
assert(entry.title == "One" and entry.isDir == false, "the song's fields")

-- Other users see only their own.
assert(#now_playing(neighbour) == 0, "the neighbour sees nothing of the listener's")

-- A played song counts once it stops, and a stopped one no longer plays.
play_through("one")
assert(play_count("one") == 1, `one counts, got {play_count("one")}`)
assert(#now_playing() == 0, "a stopped song")

-- A play started with ignoreScrobble counts nothing, however its later reports declare it.
report("two", "starting", 0, { ignoreScrobble = "true" })
play_through("two", { ignoreScrobble = "false" })
assert(play_count("two") == 0, `two counts nothing, got {play_count("two")}`)

-- A "now playing" scrobble shows its song without counting it, until the client's own start of
-- the song, which counts.
call.ok("scrobble", key, { id = ids.three, submission = "false", c = "Phone" })
local notified = now_playing()
assert(#notified == 1 and notified[1].id == ids.three and notified[1].state == "playing", "a notified song")
report("three", "starting", 0)
play_through("three")
assert(play_count("three") == 1, `three counts after its notification, got {play_count("three")}`)

-- A notification of a song the client already reports leaves its play counting.
report("four", "starting", 0)
report("four", "playing", 0)
call.ok("scrobble", key, { id = ids.four, submission = "false", c = "Phone" })
harness.task:wait(3.2)
report("four", "stopped", 3200)
assert(play_count("four") == 1, `four counts despite its notification, got {play_count("four")}`)

-- A starting report arriving after the client's playing one leaves the song playing, so it counts.
report("five", "playing", 0)
report("five", "starting", 0)
assert(now_playing()[1].state == "playing", "a late start keeps the song playing")
harness.task:wait(3.2)
report("five", "stopped", 3200)
assert(play_count("five") == 1, `five counts despite its late start, got {play_count("five")}`)

-- Positions move at the playback rate, which keeps to the host's range.
report("long", "playing", 10000, { playbackRate = "2" })
local fast = now_playing()[1]
assert(fast.playbackRate == 2 and fast.positionMs >= 10000, `rate {fast.playbackRate} at {fast.positionMs}`)
report("long", "playing", 10000, { playbackRate = "10" })
assert(now_playing()[1].playbackRate == 4, "a rate past the host's fastest")
report("long", "paused", 12000)
local paused = now_playing()[1]
assert(paused.state == "paused" and paused.positionMs == 12000, `paused at {paused.positionMs}`)

-- Each client is a player of its own.
call.ok("reportPlayback", key, {
	mediaId = ids.one,
	mediaType = "song",
	positionMs = "0",
	state = "playing",
	c = "Desktop",
})
local players = now_playing()
assert(#players == 2, `two players, got {#players}`)
assert(players[1].playerName == "Desktop" and players[2].playerName == "Phone", "the newest report first")
assert(players[1].playerId ~= players[2].playerId, "distinct player ids")

-- Devices running the same app under one name are players of their own where their User-Agents
-- differ.
local function report_from(agent: string, track: string)
	local response = call.raw("reportPlayback", {
		apiKey = key,
		f = "json",
		mediaId = ids[track],
		mediaType = "song",
		positionMs = "0",
		state = "playing",
		c = "Tablet",
	}, { headers = { ["User-Agent"] = agent } })
	local body = (serde.json.decode(call.body(response)) :: any)["subsonic-response"]
	assert(body.status == "ok", `reportPlayback from {agent}`)
end
report_from("Player/1.0 (Pixel)", "one")
report_from("Player/1.0 (iPad)", "two")
report_from("Player/1.0 (Pixel)", "three")
local tablets = {}
for _, playing in now_playing() do
	if playing.playerName == "Tablet" then
		table.insert(tablets, playing.id)
	end
end
call.same(tablets, { ids.three, ids.two }, "one player per User-Agent")

local function failure(params: { [string]: string }): number
	local full: { [string]: string } = { mediaId = ids.long, mediaType = "song", positionMs = "0", state = "playing" }
	for name, value in params do
		full[name] = if value == "" then nil :: any else value
	end
	return call.error_code("reportPlayback", key, full)
end
assert(failure({ mediaId = "" }) == 10, "a media id is required")
assert(failure({ mediaType = "" }) == 10, "a media type is required")
assert(failure({ mediaType = "podcast" }) == 0, "only songs play")
assert(failure({ positionMs = "" }) == 10, "a position is required")
assert(failure({ positionMs = "-1" }) == 0, "a negative position")
assert(failure({ state = "" }) == 10, "a state is required")
assert(failure({ state = "seeking" }) == 0, "an unknown state")
assert(failure({ playbackRate = "0" }) == 0, "a rate of 0")
assert(failure({ playbackRate = "fast" }) == 0, "a rate is a number")
assert(failure({ ignoreScrobble = "maybe" }) == 0, "ignoreScrobble is a boolean")
assert(failure({ mediaId = ids.hidden }) == 70, "a hidden song")
