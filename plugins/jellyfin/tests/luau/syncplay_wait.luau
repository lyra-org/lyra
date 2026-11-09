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

-- The `Command` and `State/Reason` messages a device was sent since the last look.
local function received(device: string): string
	local seen = {}
	for _, message in inboxes[device] do
		local data = message.Data
		if message.MessageType == "SyncPlayCommand" then
			table.insert(seen, data.Command)
		elseif message.MessageType == "SyncPlayGroupUpdate" and data.Type == "StateUpdate" then
			table.insert(seen, `{data.Data.State}/{data.Data.Reason}`)
		end
	end
	table.clear(inboxes[device])
	return table.concat(seen, ",")
end

local function check(actual: string, expected: string, why: string)
	assert(actual == expected, `{why}: expected "{expected}", got "{actual}"`)
end

syncplay.group_wait_seconds = 0.2

local group_id = post("a", "New", { GroupName = "Party" }).body.GroupId
post("b", "Join", { GroupId = group_id })
received("a")
received("b")

-- A group resuming playback starts without the members that never report ready.
post("a", "SetNewQueue", { PlayingQueue = { item_ids.encode(fixture.ids.song) } })
post("a", "Ready", { IsPlaying = true })
received("a")
received("b")
task.wait(0.3)
check(received("a"), "Unpause,Playing/Unpause", "a resuming group unpauses after the wait")
check(received("b"), "Unpause,Playing/Unpause", "the whole group hears it")

-- A paused group stays paused where it waited.
post("a", "Pause")
post("b", "Buffering", { IsPlaying = false })
received("a")
task.wait(0.3)
check(received("a"), "Pause,Paused/Pause", "a paused group pauses after the wait")

-- Members buffering again don't push the deadline back.
post("a", "Unpause")
post("b", "Buffering", { IsPlaying = true })
task.wait(0.12)
post("b", "Buffering", { IsPlaying = true })
received("a")
task.wait(0.14)
check(received("a"), "Unpause,Playing/Unpause", "the wait runs from when it started")

-- Leaving the waiting state cancels the wait, so the next one runs in full.
post("a", "Pause")
post("a", "Unpause")
post("b", "Buffering", { IsPlaying = true })
task.wait(0.12)
post("b", "Ready", { IsPlaying = true })
post("a", "Pause")
post("b", "Buffering", { IsPlaying = true })
received("a")
task.wait(0.14)
check(received("a"), "", "the earlier wait doesn't fire")
task.wait(0.1)
check(received("a"), "Pause,Paused/Pause", "the new wait does")

-- A paused group told to resume while it waits resumes once everyone is ready.
post("a", "Pause")
post("b", "Buffering", { IsPlaying = false })
received("a")
post("a", "Unpause")
check(received("a"), "Waiting/Unpause", "the group keeps waiting")
post("b", "Ready", { IsPlaying = false })
check(received("a"), "Unpause,Playing/Ready", "and resumes with the member")

-- A group told to resume while it already means to starts at once, without the member it waits
-- for, whose buffering it then ignores.
post("b", "Buffering", { IsPlaying = true })
received("a")
post("a", "Unpause")
check(received("a"), "Unpause,Playing/Unpause", "the group starts at once")
post("b", "Buffering", { IsPlaying = true })
check(received("a"), "", "a member buffering doesn't stop it")
post("a", "Pause")
post("a", "Unpause")
post("b", "Buffering", { IsPlaying = true })
check(received("a"), "Pause,Paused/Pause,Unpause,Playing/Unpause,Pause,Waiting/Buffer", "until its state changes")
post("b", "Ready", { IsPlaying = true })
received("a")
received("b")

-- A stopped group plays its item again from the start once everyone is ready.
post("a", "Stop")
received("a")
received("b")
post("a", "Unpause")
check(received("a"), "", "the group waits for everyone")
post("a", "Ready", { IsPlaying = false, PositionTicks = 0 })
post("b", "Ready", { IsPlaying = false, PositionTicks = 0 })
check(received("b"), "Unpause,Playing/Ready", "then plays")
received("a")

-- A group paused while it waits is paused once everyone is ready.
post("a", "Unpause")
post("b", "Buffering", { IsPlaying = true })
received("a")
post("a", "Pause")
check(received("a"), "Waiting/Pause", "the group keeps waiting")
post("b", "Ready", { IsPlaying = false })
check(received("a"), "Pause,Paused/Pause", "then pauses")

-- A group that was paused before the wait returns to being paused.
post("b", "Buffering", { IsPlaying = false })
received("a")
post("b", "Ready", { IsPlaying = false })
check(received("a"), "Pause,Paused/Ready", "the group is paused again")

-- A group that no longer waits for anyone resumes if it means to.
post("a", "Unpause")
post("b", "Buffering", { IsPlaying = true })
received("a")
post("b", "SetIgnoreWait", { IgnoreWait = true })
check(received("a"), "Unpause,Playing/Unpause", "the group resumes without the member")
post("b", "SetIgnoreWait", { IgnoreWait = false })

-- Outside playback, a pause only reminds the member of the group's state.
post("a", "Pause")
received("a")
received("b")
post("b", "Pause")
check(received("a"), "", "a paused group is left alone")
check(received("b"), "Pause", "the member hears it is paused")
post("a", "Stop")
received("a")
received("b")
post("b", "Pause")
check(received("b"), "Stop", "a stopped group tells the member to stop")

-- A disbanded group never times out.
post("b", "Buffering", { IsPlaying = true })
post("a", "Leave")
post("b", "Leave")
received("a")
received("b")
task.wait(0.3)
check(received("a") .. received("b"), "", "a disbanded group is silent")

-- A queue starting past the end of its item starts at the end, so members reporting ready there
-- aren't sent back.
group_id = post("a", "New", { GroupName = "Party" }).body.GroupId
post("b", "Join", { GroupId = group_id })
post("a", "SetNewQueue", {
	PlayingQueue = { item_ids.encode(fixture.ids.song) },
	StartPositionTicks = 100 * 10000000,
})
received("a")
received("b")
post("a", "Ready", { IsPlaying = false, PositionTicks = 60 * 10000000 })
post("b", "Ready", { IsPlaying = false, PositionTicks = 60 * 10000000 })
check(received("a"), "Pause,Unpause,Playing/Ready", "the group starts at the end of the item")
received("b")
post("a", "Leave")
post("b", "Leave")
received("a")
received("b")

-- A group paused while its first queue loads tells the members once everyone is ready.
group_id = post("a", "New", { GroupName = "Party" }).body.GroupId
post("b", "Join", { GroupId = group_id })
post("a", "SetNewQueue", { PlayingQueue = { item_ids.encode(fixture.ids.song) } })
post("a", "Pause")
received("a")
received("b")
post("a", "Ready", { IsPlaying = false })
post("b", "Ready", { IsPlaying = true })
check(received("a"), "Pause,Paused/Ready", "the group is paused")
check(received("b"), "Pause,Paused/Ready", "including the member that started playing")
post("a", "Leave")
post("b", "Leave")
