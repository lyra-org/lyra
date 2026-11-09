local fixture, harness = ...
local playback_sessions = require("@lyra/playback_sessions")
local serde = require("@harmony/serde")

local active_sessions = require("../../protocol/active_sessions")
local item_ids = require("../../protocol/items/identifiers")
local remote_control = require("../../routes/remote_control")

local ids = fixture.ids

local function hex(id: string): string
	return item_ids.encode(id) :: string
end

local sent: { string } = {}
active_sessions.entries.socket = {
	sender = {
		send = function(_, text: string)
			table.insert(sent, text)
		end,
		is_closed = function()
			return false
		end,
		close = function() end,
	},
	session_id = "socket",
	user_id = fixture.users.listener,
} :: any
active_sessions.entries.admins_socket = {
	sender = {
		send = function() end,
		is_closed = function()
			return false
		end,
		close = function() end,
	},
	session_id = "admins_socket",
	user_id = fixture.users.admin,
} :: any

local function post(api_key: string, session_id: string, params: { [string]: string }, json: any?): any
	local query: { [string]: { string } } = {}
	for key, value in params do
		query[key] = { value }
	end
	return harness.plugin:call(
		remote_control.handle_play,
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = `/Sessions/{session_id}/Playing`,
				headers = { authorization = `MediaBrowser Token="{api_key}"` },
				query = query,
				body_raw = "",
				json = json,
			},
			params = { sessionId = session_id },
		} :: any
	)
end

local function status(api_key: string, session_id: string, params: { [string]: string }, json: any?): number
	return post(api_key, session_id, params, json).status
end

local admin, listener = fixture.api_keys.admin_key, fixture.api_keys.listener_key
local open = hex(ids.open)
local first, second = hex(ids.open1), hex(ids.open2)

-- The parameters are upstream's: query only, `playCommand` and `itemIds` required.
assert(status(admin, "socket", { itemIds = open }) == 400, "playCommand is required")
assert(status(admin, "socket", { playCommand = "PlayNow" }) == 400, "itemIds is required")
assert(status(admin, "socket", { playCommand = "Jump", itemIds = open }) == 400, "an unknown command is refused")
assert(status(admin, "socket", {}, { ItemIds = { open }, PlayCommand = "PlayNow" }) == 400, "the body is not read")
assert(
	status(admin, "socket", { playCommand = "PlayNow", itemIds = open, startIndex = "two" }) == 400,
	"a malformed number is refused"
)
assert(#sent == 0, "none of those reach the session")

-- An admin's command to another user's socket expands as that user and names who sent it.
assert(status(admin, "socket", {
	playCommand = "playnext",
	itemIds = `{open},{hex(ids.hidden)},{hex(ids.hidden1)}`,
	startPositionTicks = "50000",
	startIndex = "1",
	mediaSourceId = "source",
	audioStreamIndex = "2",
	subtitleStreamIndex = "-1",
}) == 204, "the command is delivered")
local message = serde.json.decode(sent[1])
assert(message.MessageType == "Play", message.MessageType)
assert(string.match(message.MessageId, "^%x+$") and #message.MessageId == 32, message.MessageId)
local data = message.Data
assert(data.PlayCommand == "PlayNext", data.PlayCommand)
assert(
	table.concat(data.ItemIds, ",") == `{first},{second}`,
	`only the tracks the session's user sees: {table.concat(data.ItemIds, ",")}`
)
assert(data.ControllingUserId == hex(fixture.users.admin), data.ControllingUserId)
assert(data.StartPositionTicks == 50000 and data.StartIndex == 1)
assert(data.MediaSourceId == "source" and data.AudioStreamIndex == 2 and data.SubtitleStreamIndex == -1)

-- The same request by the session's owner.
assert(status(listener, "socket", { playCommand = "PlayNow", itemIds = open }) == 204)
assert(serde.json.decode(sent[2]).MessageId ~= message.MessageId, "each message has its own id")
local own = serde.json.decode(sent[2]).Data
assert(own.ControllingUserId == hex(fixture.users.listener))
assert(own.StartIndex == nil and own.MediaSourceId == nil, "unset fields stay out")

assert(status(listener, "admins_socket", { playCommand = "PlayNow", itemIds = open }) == 403, "not another user's")
assert(status(admin, "missing", { playCommand = "PlayNow", itemIds = open }) == 404, "no such session")

-- A connection of Lyra's own clients takes the queue as a `play_tracks` command.
local speaker, knob = fixture.connections.speaker, fixture.connections.knob
assert(status(listener, speaker, { playCommand = "PlayLast", itemIds = open, startIndex = "1" }) == 204)
assert(status(listener, knob, { playCommand = "PlayNow", itemIds = open }) == 409, "knob did not declare play_tracks")
assert(
	status(listener, speaker, { playCommand = "PlayNow", itemIds = open, startIndex = "2" }) == 400,
	"a start index past the queue is refused"
)
assert(
	status(listener, speaker, { playCommand = "PlayNow", itemIds = hex(ids.hidden1) }) == 204,
	"nothing the user can see plays nothing"
)

-- A foreign connection isn't controllable, in the listing and when commanded.
assert(status(admin, speaker, { playCommand = "PlayNow", itemIds = open }) == 404, "another user's connection")

local function start_index_of(item_ids_param: string, index: number): (any, any)
	local count = #sent
	assert(
		status(admin, "socket", { playCommand = "PlayNow", itemIds = item_ids_param, startIndex = tostring(index) })
			== 204
	)
	assert(#sent == count + 1)
	local played = serde.json.decode(sent[#sent]).Data
	return played.StartIndex, played.ItemIds
end

-- The start follows its track past an item the session's user can't see.
local hidden_track = hex(ids.hidden1)
local start, queued = start_index_of(`{hidden_track},{first},{second}`, 1)
assert(start == 0 and queued[1] == first and #queued == 2, "removal before the start")
start, queued = start_index_of(`{first},{hidden_track},{second}`, 1)
assert(start == 1 and queued[2] == second and #queued == 2, "the start went: the next track")
start, queued = start_index_of(`{first},{second},{hidden_track}`, 1)
assert(start == 1 and queued[2] == second and #queued == 2, "removal after the start")
start, queued = start_index_of(`{first},{hidden_track}`, 1)
assert(start == 0 and #queued == 1, "the start went with nothing after: the last before it")

-- A connection's own user drops such an item while expanding, and the start follows it there too.
assert(
	status(listener, speaker, { playCommand = "PlayNow", itemIds = `{hidden_track},{first}`, startIndex = "1" }) == 204,
	"unmoved, the index would be past the queue"
)
assert(
	status(listener, speaker, { playCommand = "PlayNow", itemIds = `{first},{hidden_track}`, startIndex = "1" }) == 204,
	"the start went: the last before it"
)

local function placement(command: string): any
	return remote_control.native_play_request("conn", { first, second }, command, 50000, 1)
end
local mapped = placement("PlayNow")
assert(mapped.connection_id == "conn" and mapped.action == "play_tracks" and mapped.placement == "now")
assert(mapped.track_ids[1] == ids.open1 and mapped.track_ids[2] == ids.open2, "public ids")
assert(mapped.start_position_ms == 5 and mapped.start_index == 1, "ticks become milliseconds")
assert(placement("PlayNext").placement == "next" and placement("PlayLast").placement == "last")

-- A socket's client toggles for itself; a connection with nothing playing has nothing to toggle.
active_sessions.entries.socket = {
	sender = {
		send = function(_, text: string)
			table.insert(sent, text)
		end,
		is_closed = function()
			return false
		end,
		close = function() end,
	},
	session_id = "socket",
	user_id = fixture.users.listener,
} :: any
local function playstate(api_key: string, session_id: string, command: string, seek_ticks: string?): number
	return harness.plugin:call(
		remote_control.handle_playstate,
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = `/Sessions/{session_id}/Playing/{command}`,
				headers = { authorization = `MediaBrowser Token="{api_key}"` },
				query = { seekPositionTicks = if seek_ticks then { seek_ticks } else nil },
				body_raw = "",
				json = nil,
			},
			params = { sessionId = session_id, command = command },
		} :: any
	).status
end
local before = #sent
assert(playstate(listener, "socket", "PlayPause") == 204)
assert(#sent == before + 1, "the toggle reaches the socket")
local toggle = serde.json.decode(sent[#sent])
assert(toggle.MessageType == "Playstate" and toggle.Data.Command == "PlayPause", sent[#sent])
assert(toggle.Data.ControllingUserId == hex(fixture.users.listener), toggle.Data.ControllingUserId)
assert(playstate(fixture.api_keys.admin_key, "socket", "PlayPause") == 204, "an admin may too")
assert(#sent == before + 2, "and it is forwarded the same")
assert(playstate(listener, speaker, "PlayPause") == 204, "nothing to toggle")
assert(#sent == before + 2, "nothing goes to a socket")

-- A name is read in any case and sent on in upstream's spelling; any other is refused.
assert(playstate(listener, "socket", "seek", "70000") == 204, "jellyfin-web's spelling")
local seek = serde.json.decode(sent[#sent]).Data
assert(seek.Command == "Seek" and seek.SeekPositionTicks == 70000, sent[#sent])
assert(playstate(listener, "socket", "nextTrack") == 204)
assert(serde.json.decode(sent[#sent]).Data.Command == "NextTrack", sent[#sent])
assert(playstate(listener, "socket", "Eject") == 400, "an unknown command is refused")
assert(#sent == before + 4, "and reaches no session")

active_sessions.entries.admins_socket = {
	sender = {
		send = function() end,
		is_closed = function()
			return false
		end,
		close = function() end,
	},
	session_id = "admins_socket",
	user_id = fixture.users.admin,
} :: any
assert(playstate(listener, "admins_socket", "PlayPause") == 403, "not another user's")
active_sessions.entries.socket = nil
active_sessions.entries.admins_socket = nil

-- A playstate command for a connection goes straight to it, and core refuses what it may not take.
assert(playstate(listener, knob, "Pause") == 204, "declared")
assert(playstate(listener, knob, "pause") == 204, "in any case")
assert(playstate(listener, knob, "Stop") == 409, "undeclared")
assert(playstate(listener, knob, "stop") == 409, "in any case")
assert(playstate(listener, "no-such-connection", "Pause") == 404, "a target that isn't there")
assert(playstate(admin, speaker, "Pause") == 403, "another user's connection")

-- Past a connection's limit the queue is cut, keeping the start playable.
local queue_of = function(count: number): { string }
	local queue = {}
	for index = 1, count do
		queue[index] = `t{index}`
	end
	return queue
end
local cut, cut_start = remote_control.native_queue_window(queue_of(5), nil, 3)
assert(table.concat(cut, ",") == "t1,t2,t3" and cut_start == nil, "the beginning, without a start")
cut, cut_start = remote_control.native_queue_window(queue_of(5), 1, 3)
assert(table.concat(cut, ",") == "t1,t2,t3" and cut_start == 1, "the beginning holds the start")
cut, cut_start = remote_control.native_queue_window(queue_of(10), 4, 3)
assert(table.concat(cut, ",") == "t5,t6,t7" and cut_start == 0, "opens on a start past the beginning")
cut, cut_start = remote_control.native_queue_window(queue_of(5), 4, 3)
assert(table.concat(cut, ",") == "t3,t4,t5" and cut_start == 2, "within the queue's end")
cut, cut_start = remote_control.native_queue_window(queue_of(5), 5, 3)
assert(#cut == 3 and cut_start == 3, "a start past the queue stays past the result")
cut, cut_start = remote_control.native_queue_window(queue_of(3), 2, 3)
assert(#cut == 3 and cut_start == 2, "a queue within the limit is untouched")

-- Through the route, with the same track named past the real limit.
local over = table.create(playback_sessions.MAX_PLAY_TRACK_IDS + 1, first)
local over_csv = table.concat(over, ",")
assert(status(listener, speaker, { playCommand = "PlayNow", itemIds = over_csv }) == 204, "past the limit still plays")
assert(status(listener, speaker, {
	playCommand = "PlayNow",
	itemIds = over_csv,
	startIndex = tostring(playback_sessions.MAX_PLAY_TRACK_IDS),
}) == 204, "and so does a start past it")
assert(status(listener, speaker, {
	playCommand = "PlayNow",
	itemIds = over_csv,
	startIndex = tostring(playback_sessions.MAX_PLAY_TRACK_IDS + 1),
}) == 400, "a start past the queue is refused")

-- The general commands: to a socket as sent, and to a connection as the command it declares.
local function general(handler: (any) -> any, api_key: string, params: any, json: any?): number
	return harness.plugin:call(
		handler,
		{
			plugin_id = "jellyfin",
			request = {
				method = "POST",
				path = "/Sessions/Command",
				headers = { authorization = `MediaBrowser Token="{api_key}"` },
				query = {},
				body_raw = "",
				json = json,
			},
			params = params,
		} :: any
	).status
end

local commands: { string } = {}
active_sessions.entries.socket = {
	sender = {
		send = function(_, text: string)
			table.insert(commands, text)
		end,
		is_closed = function()
			return false
		end,
		close = function() end,
	},
	session_id = "socket",
	user_id = fixture.users.listener,
} :: any
assert(general(remote_control.handle_named_command, listener, { sessionId = "socket", command = "GoHome" }) == 204)
local named = serde.json.decode(commands[1])
assert(named.MessageType == "GeneralCommand" and named.Data.Name == "GoHome", commands[1])
assert(named.Data.ControllingUserId == hex(fixture.users.listener))
assert(
	general(
		remote_control.handle_general_command,
		listener,
		{ sessionId = "socket" },
		{ Name = "DisplayMessage", Arguments = { Header = "Hi" } }
	) == 204
)
assert(serde.json.decode(commands[2]).Data.Arguments.Header == "Hi")
assert(string.find(commands[1], '"Arguments":{}', 1, true), commands[1])
assert(
	general(
		remote_control.handle_general_command,
		listener,
		{ sessionId = "socket" },
		{ Name = "GoHome", Arguments = {} }
	) == 204
)
assert(string.find(commands[3], '"Arguments":{}', 1, true), commands[3])
assert(general(remote_control.handle_named_command, listener, { sessionId = "socket", command = "gohome" }) == 204)
assert(serde.json.decode(commands[4]).Data.Name == "GoHome", "a name is sent on in upstream's spelling")
assert(
	general(remote_control.handle_general_command, listener, { sessionId = "socket" }, { Name = "setvolume" }) == 204
)
assert(serde.json.decode(commands[5]).Data.Name == "SetVolume", commands[5])
assert(
	general(remote_control.handle_named_command, listener, { sessionId = "socket", command = "Dance" }) == 400,
	"an unknown command is refused"
)
assert(general(remote_control.handle_general_command, listener, { sessionId = "socket" }, { Name = "Dance" }) == 400)
assert(#commands == 5, "and reaches no session")
active_sessions.entries.socket = nil

local function set_volume(volume: string): number
	return general(
		remote_control.handle_general_command,
		listener,
		{ sessionId = speaker },
		{ Name = "SetVolume", Arguments = { Volume = volume } }
	)
end
assert(set_volume("50") == 409, "the speaker didn't declare set_volume")
