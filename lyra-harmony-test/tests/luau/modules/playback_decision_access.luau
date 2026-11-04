local fixture, harness = ...

local playback_sources = require("@lyra/playback_sources")

-- The viewer cannot see the track's library, so a decision for it is refused like its stream.
local ok, err = pcall(playback_sources.decide, fixture.ids.song)
assert(not ok, "a caller without access gets no decision")
assert(string.find(tostring(err), "404", 1, true), `error {err}`)

-- A public decision answers as a public stream route serves: without the caller's access check.
local public = playback_sources.decide(fixture.ids.song, { public = true })
assert(public.transcode == false, "the source streams as it is")

-- A handler that authenticated no one decides as its stream would be served: a public stream
-- serves, and any other is refused as unauthenticated.
local anonymous = harness.plugin:call(function()
	return playback_sources.decide(fixture.ids.song, { public = true })
end)
assert(anonymous.transcode == false, "a public stream needs no caller")
local refused_ok, refused = harness.plugin:call(function()
	return pcall(playback_sources.decide, fixture.ids.song)
end)
assert(not refused_ok, "a private stream needs a caller")
assert(string.find(tostring(refused), "401", 1, true), `error {refused}`)
