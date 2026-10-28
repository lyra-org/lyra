--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local serde = require("@harmony/serde")
local task = require("@harmony/task")
local json = serde.json

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

export type Envelope = {
	MessageType: string,
	Data: JsonValue,
	MessageId: string?,
}

export type KeepAlive = {
	-- os.clock() of the socket's last KeepAlive.
	last_keep_alive_s: number,
}

local module = {}

-- As upstream, a socket that sends no KeepAlive for this many seconds is lost. `ForceKeepAlive`
-- tells clients this, and they send one at half that rate.
module.lost_seconds = 60

function module.decode(text: string): Envelope?
	local ok, decoded = pcall(json.decode, text)
	if not ok or type(decoded) ~= "table" then
		return nil
	end
	local obj = decoded :: JsonObject

	local message_type = obj.MessageType
	if type(message_type) ~= "string" then
		return nil
	end

	local message_id: string? = nil
	local raw_message_id = obj.MessageId
	if type(raw_message_id) == "string" then
		message_id = raw_message_id
	end

	return {
		MessageType = message_type,
		Data = obj.Data,
		MessageId = message_id,
	}
end

function module.encode(message_type: string, data: JsonValue, message_id: string?): string
	local envelope: JsonObject = {
		MessageType = message_type,
	}
	if data ~= nil then
		envelope.Data = data
	end
	if message_id ~= nil then
		envelope.MessageId = message_id
	end
	return json.encode(envelope)
end

-- Watches a socket as upstream does, every fifth of the lost timeout: one quiet for three quarters
-- of it is sent `ForceKeepAlive` again, and a lost one is closed, which ends its session as a
-- client's own close does.
function module.watch_keep_alive(sender: api.ApiWebSocketSender, state: KeepAlive): thread
	return task.spawn(function()
		while not sender:is_closed() do
			task.wait(module.lost_seconds * 0.2)
			if sender:is_closed() then
				break
			end
			local quiet_s = os.clock() - state.last_keep_alive_s
			local lost = quiet_s >= module.lost_seconds
			if not lost and quiet_s > module.lost_seconds * 0.75 then
				lost = not pcall(function()
					sender:send(module.encode("ForceKeepAlive", module.lost_seconds, nil))
				end)
			end
			if lost then
				pcall(function()
					sender:close()
				end)
				break
			end
		end
	end)
end

return module
