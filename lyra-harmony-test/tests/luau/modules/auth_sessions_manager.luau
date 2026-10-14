local fixture = ...
local auth = require("@lyra/auth")

local manager = fixture.users.manager
local admin = fixture.users.admin
local listener = fixture.users.listener

-- manage_users reaches every non-admin's sessions; an admin's also take the admin permission.
local everyone = auth.list_sessions({ all_users = true })
local owners = {}
for _, session in everyone do
	owners[session.user_id] = session
end
assert(#everyone == 2 and owners[manager] ~= nil and owners[listener] ~= nil and owners[admin] == nil)
assert(#auth.list_sessions({ user_id = listener }) == 1)
assert(not pcall(auth.list_sessions, { user_id = admin }))

-- An admin's session reads as absent and survives.
assert(auth.resolve_auth(fixture.sessions.admin_web) ~= nil)
local admin_session = auth.list_sessions()[1]
assert(admin_session.user_id == admin)
assert(auth.resolve_auth(fixture.sessions.manager_web) ~= nil)
assert(auth.revoke_session(admin_session.session_id) == false)
assert(auth.resolve_auth(fixture.sessions.admin_web) ~= nil)

assert(auth.resolve_auth(fixture.sessions.manager_web) ~= nil)
assert(auth.revoke_session(owners[listener].session_id) == true)
assert(auth.resolve_auth(fixture.sessions.listener_phone) == nil)
