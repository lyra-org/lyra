local fixture = ...
local auth = require("@lyra/auth")
local playlists = require("@lyra/playlists")
local users = require("@lyra/users")

-- While authentication is off, the default user is an account like any other.
local listed = {}
for _, user in users.list() do
	listed[user.user_id] = true
end
assert(listed[fixture.users.admin] and listed[fixture.users.default])

assert(#auth.list_sessions({ all_users = true }) == 2)
local named = auth.list_sessions({ user_id = fixture.users.default })
assert(#named == 1 and named[1].user_id == fixture.users.default)

assert(playlists.query({ owner_id = fixture.users.default }).total == 1)
