local fixture = ...
local auth = require("@lyra/auth")

local admin = fixture.users.admin
local other = fixture.users.other

-- The fixture's session and the one the test runs with.
local own = auth.list_sessions()
assert(#own == 2 and own[1].user_id == admin and own[2].user_id == admin)

local everyone = auth.list_sessions({ all_users = true })
assert(#everyone == 3)
local theirs = auth.list_sessions({ user_id = other })
assert(#theirs == 1 and theirs[1].user_id == other and theirs[1].client_name == "Phone")
assert(#auth.list_sessions({ user_id = "missing-user" }) == 0)

-- manage_users revokes anyone's session.
assert(auth.revoke_session(theirs[1].session_id) == true)
assert(auth.resolve_auth(fixture.sessions.other_phone) == nil)
assert(#auth.list_sessions({ all_users = true }) == 2)
