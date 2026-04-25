--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local json = require("@harmony/json")

type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

export type Envelope = {
	MessageType: string,
	Data: JsonValue,
	MessageId: string?,
}

local module = {}

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

return module
