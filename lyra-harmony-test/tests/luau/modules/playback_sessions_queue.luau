local fixture = ...

local playback_sessions = require("@lyra/playback_sessions")

local ids = fixture.ids

local function raises(request: playback_sessions.QueueSaveRequest, why: string)
	local ok = pcall(playback_sessions.save_queue, request)
	assert(not ok, why)
end

-- The newest queue, from any client, skips one with nothing left to play. A hidden current song
-- hands over to the next visible one, from its start.
local web = playback_sessions.latest_queue()
assert(web, "a queue")
assert(web.playback_id == ids.web, `latest {web.playback_id}`)
assert(web.client_name == "Lyra Web" and web.plugin_id == nil, `from {web.client_name} of {web.plugin_id}`)
assert(#web.track_ids == 2 and web.track_ids[2] == ids.two, "only visible songs")
assert(web.current_index == 1 and web.position_ms == 0, `at {web.current_index}, {web.position_ms}`)
assert(web.updated_at_ms == 1700000002000, `updated {web.updated_at_ms}`)

-- A save becomes the newest queue, stopped where the client left it.
local phone = playback_sessions.save_queue({
	client_name = "Phone",
	track_ids = { ids.one, ids.two, ids.one },
	current_index = 2,
	position_ms = 1500,
})
local saved = playback_sessions.latest_queue()
assert(saved and saved.playback_id == phone, "the save is the newest queue")
assert(#saved.track_ids == 3 and saved.track_ids[3] == ids.one, "the queue keeps its repeats")
assert(saved.current_index == 2 and saved.position_ms == 1500, `at {saved.current_index}, {saved.position_ms}`)
assert(saved.client_name == "Phone" and saved.plugin_id == "test", `from {saved.client_name} of {saved.plugin_id}`)

-- The same client, however its name is spelled, saves to the same playback, within the song.
local again = playback_sessions.save_queue({
	client_name = "  Phone\u{200B} ",
	track_ids = { ids.one, ids.two, ids.one },
	current_index = 2,
	position_ms = 999999,
})
assert(again == phone, "a client saves to its own playback")
local clamped = playback_sessions.latest_queue()
assert(clamped and clamped.position_ms == 200000, "the position stays within the song")
local moved = playback_sessions.save_queue({ client_name = "Phone", track_ids = { ids.two }, current_index = 0 })
assert(moved == phone, "a new queue replaces the client's")
local replaced = playback_sessions.latest_queue()
assert(replaced and #replaced.track_ids == 1 and replaced.position_ms == 0, "the position defaults to 0")

-- A new queue needs every song visible, a current one among them, and no more than the limit.
raises({ client_name = "Phone", track_ids = { ids.one, ids.hidden }, current_index = 0 }, "a hidden song")
raises({ client_name = "Phone", track_ids = { ids.one, "missing" }, current_index = 0 }, "a missing song")
raises({ client_name = "Phone", track_ids = { ids.album }, current_index = 0 }, "an album")
raises({ client_name = "Phone", track_ids = {}, current_index = 0 }, "an empty queue")
raises({ client_name = "Phone", track_ids = { ids.one }, current_index = 1 }, "current past the queue")
local too_long = table.create(playback_sessions.MAX_QUEUE_TRACK_IDS + 1, ids.one)
raises({ client_name = "Phone", track_ids = too_long, current_index = 0 }, "too long a queue")
local still = playback_sessions.latest_queue()
assert(still and still.playback_id == phone and #still.track_ids == 1, "failed saves change nothing")

-- The queue a client saved before a song was hidden saves again, though only its visible songs
-- come back.
local tablet = playback_sessions.save_queue({
	client_name = "Tablet",
	track_ids = { ids.one, ids.hidden },
	current_index = 0,
	position_ms = 700,
})
assert(tablet == ids.tablet, "the seeded save is the client's")
local visible = playback_sessions.latest_queue()
assert(visible and visible.playback_id == ids.tablet, "the tablet's queue is the newest")
assert(#visible.track_ids == 1 and visible.track_ids[1] == ids.one, "only visible songs come back")
assert(visible.position_ms == 700, `at {visible.position_ms}`)

-- Removing a client's queue leaves the others; a second removal finds none.
assert(playback_sessions.remove_queue("Tablet"), "the tablet had a queue")
assert(not playback_sessions.remove_queue("Tablet"), "no queue left to remove")
assert(playback_sessions.remove_queue("Phone"), "the phone had a queue")
assert(not playback_sessions.remove_queue("Car"), "another plugin's client is not this plugin's")
local fallback = playback_sessions.latest_queue()
assert(fallback and fallback.playback_id == ids.web, "the newest queue left")
