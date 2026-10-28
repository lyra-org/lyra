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
post("a", "SetNewQueue", { PlayingQueue = { item_ids.encode_jellyfin_item_id(fixture.ids.song) } })
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
post("b", "Buffering", { IsPlaying = true })
task.wait(0.12)
post("a", "Pause")
post("b", "Buffering", { IsPlaying = true })
received("a")
task.wait(0.14)
check(received("a"), "", "the earlier wait doesn't fire")
task.wait(0.1)
check(received("a"), "Pause,Paused/Pause", "the new wait does")

-- A disbanded group never times out.
post("b", "Buffering", { IsPlaying = true })
post("a", "Leave")
post("b", "Leave")
received("a")
received("b")
task.wait(0.3)
check(received("a") .. received("b"), "", "a disbanded group is silent")
