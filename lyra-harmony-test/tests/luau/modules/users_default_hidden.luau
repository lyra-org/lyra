local fixture = ...
local auth = require("@lyra/auth")
local playlists = require("@lyra/playlists")
local users = require("@lyra/users")

-- While authentication is on, the default user is no account.
local listed = {}
for _, user in users.list() do
	table.insert(listed, user.id)
end
assert(#listed == 1 and listed[1] == fixture.users.admin)

local everyone = auth.list_sessions({ all_users = true })
assert(#everyone == 1 and everyone[1].user_id == fixture.users.admin)
assert(#auth.list_sessions({ user_id = fixture.users.default }) == 0)

assert(playlists.query({ owner_id = fixture.users.default }).total == 0)
