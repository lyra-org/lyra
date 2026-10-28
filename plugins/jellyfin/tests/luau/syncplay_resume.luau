local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local serde = require("@harmony/serde")
local syncplay = require("../../routes/syncplay")
local task = require("@harmony/task")

-- The group's clock reads a time the test moves.
local now = 1700000000000
syncplay.now_ms = function(): number
	return now
end

-- Each device's socket, which records the messages it is sent.
local sessions: { [string]: any } = {}
local inboxes: { [string]: { any } } = {}
local calling: string
syncplay.register(sessions, function(session_id: string, principal: any): any
	local entry = sessions[session_id]
	if not entry then
		local inbox = {}
		inboxes[calling] = inbox
		entry = {
			session_id = session_id,
			user_id = principal.user_id,
			info = { UserName = "admin" },
			sender = {
				is_closed = function()
					return false
				end,
				send = function(_, message: string)
					table.insert(inbox, serde.json.decode(message))
				end,
			},
		}
		sessions[session_id] = entry
	end
	return entry
end)

local function post(device: string, path: string, body: any?): any
	calling = device
	return syncplay.handlers[`POST /SyncPlay/{path}`]({
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/",
			headers = {
				authorization = `MediaBrowser Token="{fixture.api_keys.phone}", Client="Test", Device="{device}", DeviceId="{device}", Version="1"`,
			},
			query = {},
			body_raw = "",
			json = body or {},
		},
		params = {},
	})
end

local function unix_ms(timestamp: string): number
	return assert(syncplay.parse_timestamp(timestamp), `unexpected timestamp {timestamp}`)
end

local SECOND = 10000000

-- The commands a device was sent since the last look, each with how far ahead of its sending
-- it is scheduled, as `Command+ms`, and with `positions`, the position it carries in seconds, as
-- `Command@s+ms`.
local function commands(device: string, positions: boolean?): string
	local seen = {}
	for _, message in inboxes[device] do
		if message.MessageType == "SyncPlayCommand" then
			local data = message.Data
			local ahead = unix_ms(data.When) - unix_ms(data.EmittedAt)
			local at = if positions then `@{data.PositionTicks / SECOND}` else ""
			table.insert(seen, `{data.Command}{at}{if ahead < 0 then "" else "+"}{ahead}`)
		end
	end
	table.clear(inboxes[device])
	return table.concat(seen, ",")
end

-- Where, in seconds, the play queue a device was last sent starts, since the last look.
local function queue_start(device: string): string
	local start = "none"
	for _, message in inboxes[device] do
		local data = message.Data
		if message.MessageType == "SyncPlayGroupUpdate" and data.Type == "PlayQueue" then
			start = tostring(data.Data.StartPositionTicks / SECOND)
		end
	end
	return start
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

local group_id = post("a", "New", { GroupName = "Party" }).body.GroupId
post("b", "Join", { GroupId = group_id })
post("a", "SetNewQueue", { PlayingQueue = { item_ids.encode_jellyfin_item_id(fixture.ids.song) } })
post("a", "Ready", { IsPlaying = false, PositionTicks = 0 })
commands("a")
commands("b")

-- Without reported pings, the group starts twice the default ping ahead.
post("b", "Ready", { IsPlaying = false, PositionTicks = 0 })
check(commands("a"), "Unpause+1000", "the group starts once everyone is ready")
check(commands("b"), "Unpause+1000", "the whole group hears it")

post("a", "Pause")
check(commands("a"), "Pause+0", "a pause takes effect right away")
commands("b")
post("a", "Unpause")
check(commands("a"), "Unpause+1000", "an unpause starts ahead")
check(commands("b"), "Unpause+1000", "for the whole group")

-- A member that is already playing only lost track, so it alone hears when the group started.
post("b", "Unpause")
check(commands("a"), "", "the rest of the group isn't rescheduled")
check(commands("b"), "Unpause+1000", "the member hears the scheduled start")

-- The highest ping sets the delay.
post("b", "Ping", { Ping = 3000 })
post("a", "Pause")
commands("a")
commands("b")
post("a", "Unpause")
check(commands("a"), "Unpause+6000", "a slow member delays the start")
check(commands("b"), "Unpause+6000", "for everyone")

-- But never below the default ping.
post("a", "Ping", { Ping = 0 })
post("b", "Ping", { Ping = 0 })
post("a", "Pause")
commands("a")
commands("b")
post("a", "Unpause")
check(commands("a"), "Unpause+500", "fast members still start ahead")

-- A member that recovers behind the group has the others resume as it catches up. The group
-- started half a second ahead and has played 10 seconds since.
post("a", "Ping", { Ping = 500 })
post("b", "Ping", { Ping = 500 })
now += 10500
post("b", "Buffering", { IsPlaying = true, PositionTicks = 10 * SECOND })
commands("a")
commands("b")
post("b", "Ready", { IsPlaying = true, PositionTicks = 7 * SECOND })
check(commands("a"), "Unpause+3000", "the others wait for it to catch up")
check(commands("b"), "", "the recovering member keeps playing")

-- Paused, it resumes with the rest.
post("b", "Buffering", { IsPlaying = true, PositionTicks = 10 * SECOND })
commands("a")
post("b", "Ready", { IsPlaying = false, PositionTicks = 7 * SECOND })
check(commands("a"), "Unpause+3000", "the group resumes ahead")
check(commands("b"), "Unpause+3000", "with the recovered member")

-- A member just behind resumes with the others after the usual delay.
post("b", "Buffering", { IsPlaying = true, PositionTicks = 10 * SECOND })
commands("a")
post("b", "Ready", { IsPlaying = true, PositionTicks = 10 * SECOND - 5000000 })
check(commands("a"), "Unpause+1000", "a small lag waits the usual delay")
check(commands("b"), "Unpause+1000", "for everyone")

-- A group that gives up waiting starts ahead too.
syncplay.group_wait_seconds = 0.1
post("b", "Buffering", { IsPlaying = true, PositionTicks = 10 * SECOND })
commands("a")
commands("b")
task.wait(0.2)
check(commands("a"), "Unpause+1000", "the timed out group starts ahead")

-- The group keeps a clock that runs while it plays.
do
	local song = item_ids.encode_jellyfin_item_id(fixture.ids.song)
	local clock_group = post("c", "New", { GroupName = "Clock" }).body.GroupId
	post("d", "Join", { GroupId = clock_group })
	post("c", "SetNewQueue", { PlayingQueue = { song } })
	post("c", "Ready", { IsPlaying = false, PositionTicks = 0 })
	post("d", "Ready", { IsPlaying = false, PositionTicks = 0 })
	check(commands("c", true), "Unpause@0+1000", "the group starts a second ahead")
	commands("d")

	now += 1000 + 5000
	post("c", "Pause")
	check(commands("c", true), "Pause@5+0", "a pause stops the clock where playback is")
	commands("d")

	now += 2000
	post("c", "Unpause")
	check(commands("c", true), "Unpause@5+1000", "a paused clock doesn't run")
	commands("d")

	post("e", "Join", { GroupId = clock_group })
	check(queue_start("e"), "5", "a member joining before the start hears the position it starts from")
	commands("e")
	now += 1000 + 3000
	post("e", "Leave")
	post("e", "Join", { GroupId = clock_group })
	check(queue_start("e"), "8", "a late member hears where playback is")
	check(commands("e", true), "Unpause@5-3000", "and when the group started, from where")
	post("e", "Leave")

	now += 2000
	post("d", "Buffering", { IsPlaying = true, PositionTicks = 10 * SECOND })
	check(commands("c", true), "Pause@10+0", "a member buffering stops the clock where playback is")

	post("c", "Seek", { PositionTicks = 100 * SECOND })
	check(commands("c", true), "Seek@60+0", "a seek stops at the end of the song")
	post("c", "Seek", { PositionTicks = -SECOND })
	check(commands("c", true), "Seek@0+0", "and at its start")
	post("c", "Leave")
	post("d", "Leave")
end

-- Client times parse with or without a fraction or an offset.
check(tostring(syncplay.parse_timestamp("1970-01-02T00:00:01Z")), "86401000", "a whole second")
check(tostring(syncplay.parse_timestamp("2000-03-01T00:00:00.25Z")), "951868800250", "a fraction")
check(tostring(syncplay.parse_timestamp("2000-03-01T02:00:00+02:00")), "951868800000", "an offset")
