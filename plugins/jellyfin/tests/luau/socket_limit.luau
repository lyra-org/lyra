local fixture, harness = ...
local active_sessions = require("../../protocol/active_sessions")
local item_ids = require("../../protocol/items/identifiers")
local playback = require("../../protocol/items/playback")
local playback_sessions = require("@lyra/playback_sessions")
local session = require("../../protocol/session")
local socket_routes = require("../../routes/socket")
local syncplay = require("../../routes/syncplay")
local task = require("@harmony/task")

local entries = active_sessions.entries
local sockets = harness.sockets
local CAP = active_sessions.MAX_SOCKETS_PER_USER

syncplay.register(entries, function(session_id: string, principal: any): any
	local entry = entries[session_id]
	return if entry and entry.user_id == principal.user_id and not entry.sender:is_closed() then entry else nil
end)

local function check(actual: any, expected: any, why: string)
	assert(actual == expected, `{why}: expected {expected}, got {actual}`)
end

local function eventually(condition: () -> boolean, why: string)
	for _ = 1, 500 do
		if condition() then
			return
		end
		harness.task:wait(0.01)
	end
	error(`timed out waiting: {why}`)
end

local function context(device: string, api_key: string, json: any?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = if json then "POST" else "GET",
			path = "/",
			headers = {
				authorization = `MediaBrowser Client="Finamp", Device="{device}", DeviceId="{device}", Version="1", Token="{api_key}"`,
			},
			query = {},
			body_raw = "",
			json = json,
		},
		params = {},
	}
end

-- A connection through the real handler, which runs until its client goes away.
local function connect(device: string, api_key: string?): any
	local key = api_key or fixture.api_keys.phone
	local connection: any = { device = device, sender = sockets:open(), gone = false, done = false }
	local reader = {
		recv = function()
			while not connection.gone do
				harness.task:wait(0.02)
			end
			return nil
		end,
	}
	task.spawn(function()
		harness.plugin:call(socket_routes.handle, reader, connection.sender, context(device, key))
		connection.done = true
	end)
	eventually(function()
		for _, entry in entries do
			if entry.sender == connection.sender then
				connection.entry = entry
				return true
			end
		end
		return false
	end, `{device} to connect`)
	return connection
end

local function disconnect(connection: any)
	sockets:close(connection.sender)
	connection.gone = true
	eventually(function()
		return connection.done
	end, `{connection.device}'s handler to end`)
end

local function live_count(user_id: string): number
	local count = 0
	for _, entry in entries do
		if entry.user_id == user_id and not entry.evicted and not entry.sender:is_closed() then
			count += 1
		end
	end
	return count
end

local function admitted(connection: any): boolean
	return entries[connection.entry.session_id] == connection.entry and not connection.entry.evicted
end

local function check_evicted(connections: { any }, expected: { [any]: boolean }, why: string)
	for _, connection in connections do
		check(connection.entry.evicted, expected[connection] == true, `{why}: {connection.device} evicted`)
	end
end

local listener = fixture.users.listener
local other = fixture.users.other

local all: { any } = {}
for index = 1, CAP do
	table.insert(all, connect(`device-{index}`))
end
check(live_count(listener), CAP, "the cap holds that many")
check_evicted(all, {}, "under the cap")

local oldest = all[1]
local stranger = connect("stranger", fixture.api_keys.other_phone)

-- What the oldest session does must end with it.
local report_body = { ItemId = item_ids.encode_jellyfin_item_id(fixture.ids.song), PositionTicks = 0 }
local report_context = context(oldest.device, fixture.api_keys.phone, report_body)
check(session.handle_playback_report_route(report_context, "started").status, 204, "the oldest reports playback")
local group_context = context(oldest.device, fixture.api_keys.phone, { GroupName = "Room" })
check(syncplay.handlers["POST /SyncPlay/New"](group_context).status, 200, "the oldest starts a group")

-- The socket past the cap evicts the oldest and is admitted; the evicted one still reads as open.
local over_cap = connect("over-cap")
table.insert(all, over_cap)
check(oldest.entry.evicted, true, "the oldest is evicted")
check(oldest.entry.sender:is_closed(), false, "its close is only requested")
check(admitted(over_cap), true, "the new socket is admitted")
check(live_count(listener), CAP, "back at the cap")
check_evicted(all, { [oldest] = true }, "one over the cap")

-- Another user's sockets neither count nor are chosen.
check(admitted(stranger), true, "another user's socket stays")
check(live_count(other), 1, "another user's count is its own")

-- A socket evicted but not yet closed doesn't count, so the next connect evicts just one more.
local second = all[2]
local next_over = connect("next-over")
table.insert(all, next_over)
check_evicted(all, { [oldest] = true, [second] = true }, "two over the cap")
check(live_count(listener), CAP, "still at the cap")

-- An evicted session ends as any other: its handler releases it when the socket closes.
disconnect(oldest)
check(entries[oldest.entry.session_id], nil, "the evicted session is released")
local reporter = playback.reporter(oldest.entry.session_id)
assert(reporter and reporter.ended, "the evicted session ended")
local playing = playback_sessions.list_playbacks()[1]
assert(playing and playing.state == "paused", "the host pauses what the evicted session played")
local groups = syncplay.handlers["GET /SyncPlay/List"](context("device-3", fixture.api_keys.phone))
check(#groups.body, 0, "the evicted session left its group")
disconnect(second)

-- Reconnecting a session at the cap replaces it and evicts nothing else.
local stale = all[5]
local reconnected = connect(stale.device)
check(entries[stale.entry.session_id], reconnected.entry, "the reconnect takes the session")
check(reconnected.entry.evicted, false, "the reconnect is admitted")
check(stale.entry.evicted, false, "replacement is not eviction")
check_evicted(all, { [oldest] = true, [second] = true }, "after a reconnect")
check(live_count(listener), CAP, "a reconnect doesn't change the count")
disconnect(stale)
check(entries[stale.entry.session_id], reconnected.entry, "the replaced handler leaves the session")
all[5] = reconnected

-- A closed socket awaiting its handler doesn't count and isn't chosen, though it is the oldest.
local zombie = all[3]
sockets:close(zombie.sender)
local into_free_slot = connect("into-free-slot")
table.insert(all, into_free_slot)
check_evicted(all, { [oldest] = true, [second] = true }, "with a slot closed")
check(live_count(listener), CAP, "the new socket takes the closed one's place")

local past_zombie = connect("past-zombie")
table.insert(all, past_zombie)
check(zombie.entry.evicted, false, "a closed socket isn't chosen")
check(all[4].entry.evicted, true, "the oldest live socket is")

for _, connection in all do
	if not connection.done then
		disconnect(connection)
	end
end
disconnect(stranger)
check(next(entries), nil, "every handler released its slot")

-- The evicted and replaced sockets are told to close, once.
local function fake(session_id: string, user_id: string): any
	local closes = { count = 0 }
	local entry: any = {
		session_id = session_id,
		user_id = user_id,
		connection_order = active_sessions.next_connection_order(),
		evicted = false,
		sender = {
			is_closed = function()
				return false
			end,
			close = function()
				closes.count += 1
			end,
		},
	}
	return entry, closes
end

local first_closes
for index = 1, CAP do
	local entry, closes = fake(`fake-{index}`, "fake-user")
	active_sessions.admit(entry)
	first_closes = first_closes or closes
end
active_sessions.admit((fake("fake-over", "fake-user")))
check(first_closes.count, 1, "the evicted socket is closed")
active_sessions.admit((fake("fake-next", "fake-user")))
check(first_closes.count, 1, "an evicted socket isn't closed again")

local replaced_entry = entries["fake-next"]
local replacement = fake("fake-next", "fake-user")
active_sessions.admit(replacement)
check(entries["fake-next"], replacement, "the same session is replaced")
check(replaced_entry.evicted, false, "the replaced socket isn't evicted")
for session_id in entries do
	entries[session_id] = nil
end
