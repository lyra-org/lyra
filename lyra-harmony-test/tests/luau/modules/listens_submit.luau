local fixture = ...

local listens = require("@lyra/listens")

local started_at_ms = math.floor(os.time() * 1000) - 600000

local function play(play_id: string, track_id: string, played_ms: number): listens.SubmittedPlay
	return {
		play_id = play_id,
		track_id = track_id,
		started_at_ms = started_at_ms,
		played_ms = played_ms,
	}
end

local outcomes = listens.submit({
	play("recorded-play-0001", fixture.ids.long, 60000),
	play("recorded-play-0001", fixture.ids.short, 60000),
	play("short-play-0000001", fixture.ids.long, 29999),
	play("short-track-play-1", fixture.ids.short, 12000),
	play("hidden-track-play1", fixture.ids.hidden, 60000),
	play("missing-track-play", "missing-track", 60000),
	{
		play_id = "ancient-play-00001",
		track_id = fixture.ids.long,
		started_at_ms = 946684800000,
		played_ms = 60000,
	},
})
assert(outcomes["recorded-play-0001"] == "recorded", `first occurrence judged: {outcomes["recorded-play-0001"]}`)
assert(outcomes["short-play-0000001"] == "not_listened", "too short to count")
assert(outcomes["short-track-play-1"] == "recorded", "a fifth of a short track counts")
assert(outcomes["hidden-track-play1"] == "track_not_found", "a track the caller can't see is not found")
assert(outcomes["missing-track-play"] == "track_not_found", "a missing track is not found")
assert(outcomes["ancient-play-00001"] == "invalid_time", "a play before 2010 is invalid")

local again = listens.submit({ play("recorded-play-0001", fixture.ids.long, 90000) })
assert(again["recorded-play-0001"] == "duplicate", "a play counts once")

local stats = listens.get({ fixture.ids.long, fixture.ids.short })
assert(stats[fixture.ids.long].count == 1, `long count {stats[fixture.ids.long].count}`)
assert(stats[fixture.ids.short].count == 1, `short count {stats[fixture.ids.short].count}`)

-- Plays queued offline arrive in any order: each counts, and the latest start stays the last play.
local function play_at(play_id: string, offset_ms: number): listens.SubmittedPlay
	return {
		play_id = play_id,
		track_id = fixture.ids.long,
		started_at_ms = started_at_ms + offset_ms,
		played_ms = 60000,
	}
end
local batch = listens.submit({
	play_at("queued-play-000001", 200000),
	play_at("queued-play-000002", 400000),
	play_at("queued-play-000003", 100000),
})
for play_id, outcome in batch do
	assert(outcome == "recorded", `{play_id}: {outcome}`)
end
listens.submit({ play_at("queued-play-000004", 300000) })
local queued = listens.get({ fixture.ids.long, fixture.ids.album })
assert(queued[fixture.ids.long].count == 5, `long count {queued[fixture.ids.long].count}`)
assert(queued[fixture.ids.long].last_played_at_ms == started_at_ms + 400000, "the latest start is the last play")
assert(queued[fixture.ids.album].count == 6, `album count {queued[fixture.ids.album].count}`)
assert(queued[fixture.ids.album].last_played_at_ms == started_at_ms + 400000, "a release reads its tracks' totals")

assert(next(listens.submit({})) == nil, "an empty batch judges nothing")

local function rejects(plays: any, message: string)
	local ok, err = pcall(listens.submit, plays)
	assert(not ok, `rejected: {message}`)
	assert(string.find(tostring(err), message, 1, true), `error {err}`)
end

rejects({ play("bad id", fixture.ids.long, 60000) }, "plays[1]: malformed play_id")
rejects(
	{ play("valid-play-0000002", fixture.ids.long, 60000), play("number-track-play-1", 1 :: any, 60000) },
	"plays[2]"
)
rejects({ { play_id = "missing-fields-001" } }, "plays[1]")
rejects({ first = play("keyed-play-000001", fixture.ids.long, 60000) }, "expected an array")

local too_many = {}
for index = 1, 501 do
	table.insert(too_many, play(string.format("capped-play-%06d", index), fixture.ids.long, 60000))
end
rejects(too_many, "plays cap exceeded")

local after = listens.get({ fixture.ids.long })
assert(after[fixture.ids.long].count == 5, "rejected batches record nothing")
