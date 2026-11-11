local fixture = ...
local auth = require("@lyra/auth")

local amy = fixture.users.amy
local bob = fixture.users.bob

auth.set_session_data(fixture.sessions.amy_phone, { device = "phone" })

local function by_client(sessions)
	local found = {}
	for _, session in sessions do
		found[session.client_name or "none"] = session
	end
	return found
end

-- Without a query, the caller's own sessions, each with this plugin's data and never a token:
-- the fixture's two and the unnamed one the test runs with.
local own = auth.list_sessions()
assert(#own == 3)
local clients = by_client(own)
local phone = clients.Phone
assert(phone ~= nil and phone.user_id == amy and phone.username == "amy")
assert(phone.last_seen_at_ms == 100 and phone.created_at_ms > 0)
assert(type(phone.data) == "table" and (phone.data :: any).device == "phone")
local web = clients.Web
assert(web ~= nil and web.data == nil)
assert(clients.none ~= nil and clients.none.user_id == amy and clients.none.data == nil)
for _, session in own do
	assert((session :: any).token == nil and (session :: any).token_hash == nil)
end
assert(#auth.list_sessions({}) == 3)
assert(#auth.list_sessions({ user_id = amy }) == 3)

-- Another user's or every user's sessions need manage_users.
assert(not pcall(auth.list_sessions, { user_id = bob }))
assert(not pcall(auth.list_sessions, { all_users = true }))
assert(not pcall(auth.list_sessions, { user_id = amy, all_users = true }))
assert(not pcall(auth.list_sessions, { userId = amy }))

-- A foreign session reads as absent and survives.
assert(auth.resolve_auth(fixture.sessions.bob_tv) ~= nil)
local bob_sessions = auth.list_sessions()
assert(#bob_sessions == 1 and bob_sessions[1].user_id == bob)
assert(auth.resolve_auth(fixture.sessions.amy_web) ~= nil)
assert(auth.revoke_session(bob_sessions[1].session_id) == false)
assert(auth.resolve_auth(fixture.sessions.bob_tv) ~= nil)

-- Revoking an own session ends it.
assert(auth.resolve_auth(fixture.sessions.amy_web) ~= nil)
assert(auth.revoke_session(web.session_id) == true)
assert(auth.resolve_auth(fixture.sessions.amy_web) == nil)
assert(auth.resolve_auth(fixture.sessions.amy_phone) ~= nil)
assert(auth.revoke_session(web.session_id) == false)
assert(auth.revoke_session("missing-session") == false)
local remaining = by_client(auth.list_sessions())
assert(remaining.Web == nil)
assert(remaining.Phone.session_id == phone.session_id)
assert(remaining.none.session_id == clients.none.session_id)
