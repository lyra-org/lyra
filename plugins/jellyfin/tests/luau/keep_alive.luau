local serde = require("@harmony/serde")
local task = require("@harmony/task")
local websocket = require("../../protocol/websocket")

-- A socket that records what it is sent and whether it was closed.
local function socket()
	local socket = { sent = {} :: { string }, closed = false }
	socket.sender = {
		is_closed = function()
			return socket.closed
		end,
		send = function(_, text: string)
			table.insert(socket.sent, serde.json.decode(text).MessageType)
		end,
		close = function()
			socket.closed = true
		end,
	}
	return socket
end

local function check(actual: any, expected: any, why: string)
	assert(actual == expected, `{why}: expected {expected}, got {actual}`)
end

-- Checked every 0.2 seconds; reminded after 0.75 quiet, lost after 1.
websocket.lost_seconds = 1

local quiet = socket()
local quiet_state = { last_keep_alive_s = os.clock() }
websocket.watch_keep_alive(quiet.sender :: any, quiet_state)
task.wait(0.7)
check(#quiet.sent, 0, "a socket that just kept alive isn't reminded")
task.wait(0.2)
check(quiet.sent[1], "ForceKeepAlive", "a quiet socket is reminded")
check(quiet.closed, false, "a quiet socket isn't lost yet")
task.wait(0.3)
check(quiet.closed, true, "a lost socket is closed")

local alive = socket()
local alive_state = { last_keep_alive_s = os.clock() }
websocket.watch_keep_alive(alive.sender :: any, alive_state)
for _ = 1, 6 do
	task.wait(0.3)
	alive_state.last_keep_alive_s = os.clock()
end
check(alive.closed, false, "a socket that keeps alive stays open")
check(#alive.sent, 0, "and isn't reminded")
alive.closed = true

-- A socket that can't take the reminder is lost too.
local broken = socket()
broken.sender.send = function()
	error("gone")
end
websocket.watch_keep_alive(broken.sender :: any, { last_keep_alive_s = os.clock() - 0.8 })
task.wait(0.3)
check(broken.closed, true, "a socket that can't be reminded is closed")
