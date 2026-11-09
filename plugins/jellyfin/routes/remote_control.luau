--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Delivers remote-control commands to a session: a Jellyfin socket or a connection of Lyra's own
-- clients.

local api = require("@lyra/api")
local errors = require("@lyra/errors")
local playback_sessions = require("@lyra/playback_sessions")

local serde = require("@harmony/serde")

local active_sessions = require("../protocol/active_sessions")
local item_ids = require("../protocol/items/identifiers")
local listing = require("../protocol/items/listing")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")
local websocket = require("../protocol/websocket")

type ApiContext = api.ApiContext
type ApiQueryParams = api.ApiQueryParams
type ApiResponse = api.ApiResponse
type JsonObject = { [string]: api.JsonValue }
type SessionEntry = active_sessions.SessionEntry
type Connection = playback_sessions.ConnectionInfo
type CommandRequest = playback_sessions.SendCommandRequest

local sessions = active_sessions.entries

local module = {}

local PLAY_COMMANDS: { [string]: string } = {
	playnow = "PlayNow",
	playnext = "PlayNext",
	playlast = "PlayLast",
	playinstantmix = "PlayInstantMix",
	playshuffle = "PlayShuffle",
}

local NATIVE_PLACEMENTS: { [string]: playback_sessions.PlayPlacement } = {
	PlayNow = "now",
	PlayNext = "next",
	PlayLast = "last",
}

-- Upstream's `PlaystateCommand` and `GeneralCommandType`. It reads a name in any case, sends on its
-- own spelling, and refuses any other.
local function by_lowercase(names: { string }): { [string]: string }
	local canonical: { [string]: string } = {}
	for _, name in names do
		canonical[string.lower(name)] = name
	end
	return canonical
end

local PLAYSTATE_COMMANDS = by_lowercase({
	"Stop",
	"Pause",
	"Unpause",
	"NextTrack",
	"PreviousTrack",
	"Seek",
	"Rewind",
	"FastForward",
	"PlayPause",
})

local GENERAL_COMMANDS = by_lowercase({
	"MoveUp",
	"MoveDown",
	"MoveLeft",
	"MoveRight",
	"PageUp",
	"PageDown",
	"PreviousLetter",
	"NextLetter",
	"ToggleOsd",
	"ToggleContextMenu",
	"Select",
	"Back",
	"TakeScreenshot",
	"SendKey",
	"SendString",
	"GoHome",
	"GoToSettings",
	"VolumeUp",
	"VolumeDown",
	"Mute",
	"Unmute",
	"ToggleMute",
	"SetVolume",
	"SetAudioStreamIndex",
	"SetSubtitleStreamIndex",
	"ToggleFullscreen",
	"DisplayContent",
	"GoToSearch",
	"DisplayMessage",
	"SetRepeatMode",
	"ChannelUp",
	"ChannelDown",
	"Guide",
	"ToggleStats",
	"PlayMediaSource",
	"PlayTrailers",
	"SetShuffleQueue",
	"PlayState",
	"PlayNext",
	"ToggleOsdMenu",
	"Play",
	"SetMaxStreamingBitrate",
	"SetPlaybackOrder",
})

local function handle_send_command_error(err: unknown): ApiResponse
	local kind = errors.kind(err)
	if kind == "not_found" then
		return responses.not_found("session not found")
	end
	if kind == "forbidden" then
		return responses.forbidden()
	end
	if kind == "conflict" then
		return responses.conflict("session does not support this command")
	end
	if kind == "invalid" then
		return responses.bad_request(tostring(err))
	end
	return api.response.empty(500)
end

-- Remote-control commands aren't idempotent, so scope mismatch surfaces as
-- 403 and send failure as 404 rather than collapsing to 204.
local function resolve_ws_entry_for_dispatch(
	principal: session.Principal,
	session_id: string?
): (SessionEntry?, ApiResponse?)
	if not session_id then
		return nil, nil
	end
	local entry = sessions[session_id]
	if not entry or entry.sender:is_closed() then
		return nil, nil
	end
	if not session.can_control_user(principal, entry.user_id) then
		warn(`dispatch scope mismatch: principal={principal.user_id}, entry={entry.user_id}, session_id={session_id}`)
		return nil, responses.forbidden()
	end
	return entry, nil
end

local function dispatch_to_ws(principal: session.Principal, session_id: string?, envelope_text: string): ApiResponse?
	local entry, resolve_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if resolve_err then
		return resolve_err
	end
	if not entry then
		return nil
	end
	local ok = pcall(function()
		entry.sender:send(envelope_text)
	end)
	if not ok then
		-- Closing ends the socket's handler, which releases the session.
		pcall(function()
			entry.sender:close()
		end)
		return responses.not_found("session not found")
	end
	return api.response.empty(204)
end

local function native_target(session_id: string): Connection?
	for _, conn in playback_sessions.list_connections() do
		if conn.id == session_id then
			return conn
		end
	end
	return nil
end

local function send_to_connection(request: CommandRequest): ApiResponse
	local ok, err = pcall(playback_sessions.send_command, request)
	if not ok then
		return handle_send_command_error(err)
	end
	return api.response.empty(204)
end

local NATIVE_PLAYSTATE_REQUESTS: { [string]: (connection_id: string, position_ms: number) -> CommandRequest } = {
	Pause = function(connection_id: string): CommandRequest
		return { connection_id = connection_id, action = "pause" }
	end,
	Unpause = function(connection_id: string): CommandRequest
		return { connection_id = connection_id, action = "unpause" }
	end,
	Stop = function(connection_id: string): CommandRequest
		return { connection_id = connection_id, action = "stop" }
	end,
	NextTrack = function(connection_id: string): CommandRequest
		return { connection_id = connection_id, action = "next_track" }
	end,
	PreviousTrack = function(connection_id: string): CommandRequest
		return { connection_id = connection_id, action = "previous_track" }
	end,
	Seek = function(connection_id: string, position_ms: number): CommandRequest
		return { connection_id = connection_id, action = "seek", position_ms = position_ms }
	end,
}

-- What `PlayPause` means for a connection of Lyra's own clients, which have no toggle command:
-- the opposite of its last-known state. Nil when it plays nothing, so there is nothing to toggle.
local function resolve_play_pause(session_id: string): string?
	local target = native_target(session_id)
	local playback_info = if target then target.playback else nil
	if playback_info then
		return if playback_info.state == "paused" then "Unpause" else "Pause"
	end
	return nil
end

function module.handle_playstate(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local raw_command = if params then params.command else nil
	if not session_id or not raw_command then
		return responses.bad_request("missing session id or command")
	end
	local command = PLAYSTATE_COMMANDS[string.lower(raw_command)]
	if not command then
		return responses.bad_request("invalid playstate command")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil

	local resolved_command = command
	local seek_ticks: number? = nil
	if command == "PlayPause" then
		local entry, entry_err = resolve_ws_entry_for_dispatch(principal, session_id)
		if entry_err then
			return entry_err
		end
		-- A socket's client toggles for itself, as upstream.
		if not entry then
			local resolved = resolve_play_pause(session_id)
			if not resolved then
				return api.response.empty(204)
			end
			resolved_command = resolved
		end
	elseif command == "Seek" then
		local raw_ticks = requests.parse_query_string(query, { "seekPositionTicks", "SeekPositionTicks" }, nil)
		seek_ticks = if raw_ticks then tonumber(raw_ticks) else nil
		if not seek_ticks then
			return responses.bad_request("seekPositionTicks is required for Seek")
		end
	end

	local data: JsonObject = {
		Command = resolved_command,
		ControllingUserId = item_ids.encode(principal.user_id),
	}
	if seek_ticks then
		data.SeekPositionTicks = seek_ticks
	end
	local ws_response = dispatch_to_ws(principal, session_id, websocket.encode("Playstate", data))
	if ws_response then
		return ws_response
	end

	local native_request = NATIVE_PLAYSTATE_REQUESTS[resolved_command]
	if not native_request then
		return api.response.empty(204)
	end

	local position_ms = math.floor((seek_ticks or 0) / 10000)
	return send_to_connection(native_request(session_id, position_ms))
end

-- The whole-number query parameter under `keys`, and whether it is usable: absent or a number at
-- or above `min_value`, as upstream's binding refuses anything else.
local function query_int(query: ApiQueryParams?, keys: { string }, min_value: number?): (number?, boolean)
	if requests.parse_query_string(query, keys, nil) == nil then
		return nil, true
	end
	local value = requests.parse_query_int(query, keys, nil, min_value, nil)
	return value, value ~= nil
end

local function requested_item_ids(query: ApiQueryParams?): { string }
	local requested: { string } = {}
	local csv = requests.parse_query_string(query, { "itemIds", "ItemIds" }, nil)
	if csv then
		for _, part in string.split(csv, ",") do
			local trimmed = requests.trim(part)
			if trimmed and item_ids.decode(trimmed) then
				table.insert(requested, trimmed)
			end
		end
	end
	return requested
end

-- The `play_tracks` command a queue expanded for `Play` becomes on a connection of Lyra's own
-- clients. `queue_command` is what `listing.remote_play_queue` returned, which leaves only the
-- commands with a placement.
function module.native_play_request(
	connection_id: string,
	queue: { string },
	queue_command: string,
	start_position_ticks: number?,
	start_index: number?
): CommandRequest
	local track_ids: { string } = {}
	for _, item_id in queue do
		table.insert(track_ids, item_ids.decode(item_id) :: string)
	end
	return {
		connection_id = connection_id,
		action = "play_tracks",
		track_ids = track_ids,
		placement = NATIVE_PLACEMENTS[queue_command],
		start_index = start_index,
		start_position_ms = if start_position_ticks then math.floor(start_position_ticks / 10000) else nil,
	}
end

-- The part of a queue a connection can take: its beginning or, when the start lies past that, a
-- stretch opening on the start, so that track still plays. A start past the queue stays past the
-- result, for the connection's limits to refuse.
function module.native_queue_window(queue: { string }, start_index: number?, limit: number): ({ string }, number?)
	if #queue <= limit then
		return queue, start_index
	end
	local first = if start_index and start_index >= limit then math.min(start_index, #queue - limit) else 0
	local window = table.move(queue, first + 1, first + limit, 1, {} :: { string })
	return window, if start_index then start_index - first else nil
end

function module.handle_play(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return responses.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local query = if request then request.query else nil

	local raw_command = requests.parse_query_string(query, { "playCommand", "PlayCommand" }, nil)
	local play_command = if raw_command then PLAY_COMMANDS[string.lower(raw_command)] else nil
	if not play_command then
		return responses.bad_request("invalid playCommand")
	end
	local requested_ids = requested_item_ids(query)
	if #requested_ids == 0 then
		return responses.bad_request("missing itemIds")
	end
	local start_ticks, start_ticks_ok = query_int(query, { "startPositionTicks", "StartPositionTicks" }, 0)
	local start_index, start_index_ok = query_int(query, { "startIndex", "StartIndex" }, 0)
	local audio_stream_index, audio_stream_index_ok = query_int(query, { "audioStreamIndex", "AudioStreamIndex" }, nil)
	local subtitle_stream_index, subtitle_stream_index_ok =
		query_int(query, { "subtitleStreamIndex", "SubtitleStreamIndex" }, nil)
	if not (start_ticks_ok and start_index_ok and audio_stream_index_ok and subtitle_stream_index_ok) then
		return responses.bad_request("invalid playing parameter")
	end

	local entry, entry_err = resolve_ws_entry_for_dispatch(principal, session_id)
	if entry_err then
		return entry_err
	end
	local connection: Connection? = nil
	if not entry then
		connection = native_target(session_id)
		if not connection then
			return responses.not_found("session not found")
		end
	end

	local target_user_id = if entry then entry.user_id else (connection :: Connection).user_id
	local queue, queue_command, queue_start_index =
		listing.remote_play_queue(requested_ids, play_command, target_user_id, principal.user_id, start_index)

	if connection then
		-- Upstream sends an empty queue on; a connection takes no empty command, so nothing goes.
		if #queue == 0 then
			return api.response.empty(204)
		end
		local window, window_start =
			module.native_queue_window(queue, queue_start_index, playback_sessions.MAX_PLAY_TRACK_IDS)
		return send_to_connection(
			module.native_play_request(connection.id, window, queue_command, start_ticks, window_start)
		)
	end

	local queue_json: { api.JsonValue } = {}
	for _, id in queue do
		table.insert(queue_json, id)
	end
	local data: JsonObject = {
		PlayCommand = queue_command,
		ItemIds = queue_json,
		ControllingUserId = item_ids.encode(principal.user_id),
	}
	data.StartPositionTicks = start_ticks
	data.StartIndex = queue_start_index
	data.MediaSourceId = requests.parse_query_string(query, { "mediaSourceId", "MediaSourceId" }, nil)
	data.AudioStreamIndex = audio_stream_index
	data.SubtitleStreamIndex = subtitle_stream_index

	return dispatch_to_ws(principal, session_id, websocket.encode("Play", data))
		or responses.not_found("session not found")
end

function module.handle_named_command(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	local raw_command = if params then params.command else nil
	if not session_id or not raw_command then
		return responses.bad_request("missing session id or command")
	end
	local command = GENERAL_COMMANDS[string.lower(raw_command)]
	if not command then
		return responses.bad_request("invalid command")
	end

	-- Route-param variant has no body, so Arguments is always empty here;
	-- parameterised commands like SetVolume need POST /Sessions/{id}/Command.
	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command,
			Arguments = serde.empty_object() :: JsonObject,
			ControllingUserId = item_ids.encode(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end
	return api.response.empty(204)
end

function module.handle_general_command(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local params = if ctx then ctx.params else nil
	local session_id = if params then params.sessionId else nil
	if not session_id then
		return responses.bad_request("missing session id")
	end

	local request = if ctx then ctx.request else nil
	local request_json = if request then request.json else nil
	if type(request_json) ~= "table" then
		return responses.bad_request("invalid command request body")
	end
	local body = request_json :: JsonObject

	local raw_name = body.Name
	if type(raw_name) ~= "string" then
		return responses.bad_request("missing command Name")
	end
	local command_name = GENERAL_COMMANDS[string.lower(raw_name)]
	if not command_name then
		return responses.bad_request("invalid command Name")
	end
	local arguments: JsonObject = {}
	local raw_arguments = body.Arguments
	if type(raw_arguments) == "table" then
		arguments = raw_arguments :: JsonObject
	end

	local ws_response = dispatch_to_ws(
		principal,
		session_id,
		websocket.encode("GeneralCommand", {
			Name = command_name,
			Arguments = if next(arguments) == nil then serde.empty_object() :: JsonObject else arguments,
			ControllingUserId = item_ids.encode(principal.user_id),
		})
	)
	if ws_response then
		return ws_response
	end

	if command_name == "SetVolume" then
		local volume = tonumber(arguments.Volume)
		if volume then
			return send_to_connection({ connection_id = session_id, action = "set_volume", level = volume / 100 })
		end
	end
	return api.response.empty(204)
end

return module
