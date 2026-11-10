local fixture = ...
local users = require("@lyra/users")

local admin = fixture.users.admin
local emile = fixture.users["émile"]

-- Reads by id are keyed by id, whoever the caller is; missing users and the default user are absent.
local found = users.get({ admin, emile, emile, fixture.users.default, "missing" })
local count = 0
for _ in found do
	count += 1
end
assert(count == 2, `two users, got {count}`)
assert(found[admin].id == admin and found[admin].username == "admin" and found[admin].role == "admin")
assert(table.find(found[admin].permissions, "admin") ~= nil, "the admin's permissions")
assert(found[emile].id == emile and found[emile].username == "émile")
assert(#found[emile].permissions == 1 and found[emile].permissions[1] == "download", "a role's permissions")
assert(next(users.get({})) == nil, "no ids")

-- A username matches as sign-in matches it, whatever its case, and names at most one user.
for _, spelling in { "émile", "ÉMILE", "Émile" } do
	local named = users.list({ username = spelling })
	assert(#named == 1 and named[1].id == emile, spelling)
end
assert(#users.list({ username = "emile" }) == 0, "accents count")
assert(#users.list({ username = "nobody" }) == 0, "an unknown username")
assert(#users.list({ username = "default" }) == 0, "the default user is no account")
assert(#users.list({}) == 2 and #users.list() == 2, "without a username, every user")

assert(not pcall(users.list, { name = "admin" } :: any), "unknown options raise")
