--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")
local playback_sessions = require("@lyra/playback_sessions")

local task = require("@harmony/task")

local active_sessions = require("../protocol/active_sessions")
local constants = require("../constants")
local playback = require("../protocol/items/playback")
local session = require("../protocol/session")
local syncplay = require("./syncplay")
local user_data_changes = require("../protocol/user_data_changes")
local websocket = require("../protocol/websocket")

local P = constants.ROUTE_PREFIX

type ResolvedAuth = auth.ResolvedAuth
type SessionEntry = active_sessions.SessionEntry

local sessions = active_sessions.entries

local JELLYFIN_WS_REAUTH_INTERVAL_SECONDS = 60
local JELLYFIN_WS_REAUTH_MAX_ERRORS = 5
-- Rate-limit inbound PlaybackProgress so a misbehaving client can't flood the
-- DB. Start / Stopped go through unthrottled. Mirrors Jellyfin's own ~10s
-- progress cadence.
local JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS = 10
local JELLYFIN_SESSIONS_PUSH_SECONDS = 2

local module = {}

function module.handle(reader: api.ApiWebSocketReader, sender: api.ApiWebSocketSender, ctx: api.ApiContext)
	local resolved_auth, token = session.require_jellyfin_auth(ctx)
	if not resolved_auth or not token then
		return
	end

	local identity, identity_err = session.resolve_client_identity(ctx, resolved_auth, token)
	if not identity then
		warn(`socket rejected: {identity_err or "missing Client/DeviceId"}`)
		return
	end

	local principal = resolved_auth.principal
	local session_id = session.build_session_id(principal, identity)
	local iso_now = session.current_iso_timestamp()
	local info = session.build_session_info(session_id, principal, identity, iso_now)
	local entry: SessionEntry = {
		sender = sender,
		session_id = session_id,
		user_id = principal.user_id,
		principal = principal,
		device_id = identity.device_id,
		client_device_name = identity.device,
		info = info,
		capabilities = nil,
		subscribed_sessions = false,
		last_sessions_push_s = nil,
		last_activity = iso_now,
		last_playback_event_s = nil,
		last_keep_alive_s = os.clock(),
		connection_order = active_sessions.next_connection_order(),
		evicted = false,
		changed_items = { queue = {}, queued = {} },
	}

	local existing = sessions[session_id]
	if existing then
		if existing.sender:is_closed() then
			-- Stale zombie from a handler that hasn't run cleanup yet; drop
			-- regardless of whose it was so a legitimate reconnect isn't
			-- blocked by a dead foreign slot.
			sessions[session_id] = nil
		elseif existing.user_id ~= principal.user_id then
			-- Different-user means a collision on the id function; refuse to
			-- overwrite another user's live socket.
			warn(
				`session id collision; refusing overwrite: existing.user_id={existing.user_id}, new.user_id={principal.user_id}`
			)
			return
		end
	end
	-- Held before a replaced socket closes, so its close leaves the session's playback alone.
	playback_sessions.hold_session(sender, session_id)
	-- A socket that closed already holds nothing, so it must not replace an open one.
	if sender:is_closed() then
		return
	end
	active_sessions.admit(entry)
	active_sessions.restore_capabilities(entry)

	pcall(function()
		sender:send(websocket.encode("ForceKeepAlive", websocket.lost_seconds, nil))
	end)
	local keep_alive_thread = websocket.watch_keep_alive(sender, entry)

	-- The driver only re-auths auth-required WS routes; public routes like
	-- this one own their re-auth. Outer xpcall ensures a throw can't silently
	-- disable enforcement for the rest of the connection.
	local reauth_thread = task.spawn(function()
		xpcall(function()
			local consecutive_errors = 0
			while not sender:is_closed() do
				task.wait(JELLYFIN_WS_REAUTH_INTERVAL_SECONDS)
				if sender:is_closed() then
					break
				end
				local ok, result = pcall(auth.resolve_auth, token)
				if ok then
					if result == nil then
						sender:close()
						break
					end
					local refreshed = result :: ResolvedAuth
					if refreshed.principal.user_id ~= principal.user_id then
						sender:close()
						break
					end
					entry.principal = refreshed.principal
					entry.info.UserName = refreshed.principal.username
					consecutive_errors = 0
				else
					consecutive_errors += 1
					if consecutive_errors >= JELLYFIN_WS_REAUTH_MAX_ERRORS then
						warn(`re-auth failed repeatedly, closing (user_id={principal.user_id}): {tostring(result)}`)
						sender:close()
						break
					end
				end
			end
		end, function(err)
			-- Can't safely restart without risking runaway-loop exceptions,
			-- so close the socket and surface the failure.
			warn(`re-auth thread crashed, closing (user_id={principal.user_id}): {tostring(err)}`)
			pcall(function()
				sender:close()
			end)
		end)
	end)

	-- Spawned from this socket's dispatch, so pushes act as its principal.
	local sessions_thread = task.spawn(function()
		while not sender:is_closed() do
			task.wait(JELLYFIN_SESSIONS_PUSH_SECONDS)
			if entry.subscribed_sessions and not sender:is_closed() then
				active_sessions.push(entry)
			end
		end
	end)

	local user_data_thread = user_data_changes.watch(entry)

	-- Keep cleanup reachable on handler throw.
	xpcall(function()
		while true do
			local frame = reader:recv()
			if frame == nil then
				break
			end
			local envelope = websocket.decode(frame)
			if not envelope then
				continue
			end
			entry.last_activity = session.current_iso_timestamp()
			local mt = envelope.MessageType
			if mt == "KeepAlive" then
				entry.last_keep_alive_s = os.clock()
				pcall(function()
					sender:send(websocket.encode("KeepAlive", websocket.lost_seconds, envelope.MessageId))
				end)
			elseif mt == "SessionsStart" then
				-- Only a new subscription gets an immediate list, and no sooner than the push interval.
				if not entry.subscribed_sessions then
					entry.subscribed_sessions = true
					local last_push_s = entry.last_sessions_push_s
					if not last_push_s or os.clock() - last_push_s >= JELLYFIN_SESSIONS_PUSH_SECONDS then
						active_sessions.push(entry)
					end
				end
			elseif mt == "SessionsStop" then
				entry.subscribed_sessions = false
			elseif mt == "PlaybackStart" then
				local ok, report_err =
					session.handle_ws_playback_event(resolved_auth, envelope.Data, "started", identity)
				entry.last_playback_event_s = os.time()
				if not ok then
					warn(
						`rejected PlaybackStart (user_id={principal.user_id}): {report_err or "invalid playback report"}`
					)
				end
			elseif mt == "PlaybackProgress" then
				local now_s = os.time()
				local last_s = entry.last_playback_event_s
				if last_s and now_s - last_s < JELLYFIN_WS_PROGRESS_MIN_INTERVAL_SECONDS then
					continue
				end
				local ok, report_err =
					session.handle_ws_playback_event(resolved_auth, envelope.Data, "progress", identity)
				-- Rejected reports consume the interval too, limiting repeated diagnostics.
				entry.last_playback_event_s = now_s
				if not ok then
					warn(
						`rejected PlaybackProgress (user_id={principal.user_id}): {report_err or "invalid playback report"}`
					)
				end
			elseif mt == "PlaybackStopped" then
				local ok, report_err =
					session.handle_ws_playback_event(resolved_auth, envelope.Data, "stopped", identity)
				entry.last_playback_event_s = os.time()
				if not ok then
					warn(
						`rejected PlaybackStopped (user_id={principal.user_id}): {report_err or "invalid playback report"}`
					)
				end
			end
		end
	end, function(err)
		warn(`socket handler error (user_id={principal.user_id}): {tostring(err)}`)
	end)

	task.cancel(reauth_thread)
	task.cancel(sessions_thread)
	task.cancel(user_data_thread)
	task.cancel(keep_alive_thread)

	-- Only the handler releases its slot. A concurrent reconnect may have replaced the slot; its
	-- socket then owns the session.
	if sessions[session_id] == entry then
		syncplay.disconnect_entry(entry)
		sessions[session_id] = nil
		playback.end_session(session_id)
	end
end

function module.register()
	api.websocket(P .. "/socket", module.handle, { auth = "public" })
end

return module
