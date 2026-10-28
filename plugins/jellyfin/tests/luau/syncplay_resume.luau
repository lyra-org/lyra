local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local serde = require("@harmony/serde")
local syncplay = require("../../routes/syncplay")
local task = require("@harmony/task")

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

-- The commands a device was sent since the last look, each with how far ahead of its sending
-- it is scheduled, as `Command+ms`.
local function commands(device: string): string
	local seen = {}
	for _, message in inboxes[device] do
		if message.MessageType == "SyncPlayCommand" then
			local data = message.Data
			local ahead = unix_ms(data.When) - unix_ms(data.EmittedAt)
			-- Scheduling and sending read the clock apart, so round to the nearest 10 ms.
			table.insert(seen, `{data.Command}+{math.floor(ahead / 10 + 0.5) * 10}`)
		end
	end
	table.clear(inboxes[device])
	return table.concat(seen, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

local SECOND = 10000000

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

-- A member that recovers behind the group has the others resume as it catches up.
post("a", "Ping", { Ping = 500 })
post("b", "Ping", { Ping = 500 })
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

-- Client times parse with or without a fraction or an offset.
check(tostring(syncplay.parse_timestamp("1970-01-02T00:00:01Z")), "86401000", "a whole second")
check(tostring(syncplay.parse_timestamp("2000-03-01T00:00:00.25Z")), "951868800250", "a fraction")
check(tostring(syncplay.parse_timestamp("2000-03-01T02:00:00+02:00")), "951868800000", "an offset")
