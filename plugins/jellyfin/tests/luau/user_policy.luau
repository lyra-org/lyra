local fixture = ...
local session = require("../../protocol/session")
local users = require("@lyra/users")

-- Listed users carry their own permissions, so every route describes a user with the same policy.

local listed = {}
for _, user in users.list() do
	listed[user.user_id] = user
end

assert(listed[fixture.users.default] == nil, "the default user is no account while authentication is on")

local function policy(key: string): any
	local user = listed[fixture.users[key]]
	assert(user ~= nil, `{key} is listed`)
	return (session.to_jellyfin_user(user) :: any).Policy
end

local admin = policy("admin")
assert(admin.IsAdministrator == true, "admin is an administrator")
assert(admin.EnableContentDownloading == true, "admin implies download")

local listener = policy("listener")
assert(listener.IsAdministrator == false, "listener is not an administrator")
assert(listener.EnableContentDownloading == false, "listener lacks the download permission")

-- Upstream always reports a configured password and no easy password.
local user = session.to_jellyfin_user(listed[fixture.users.listener]) :: any
assert(user.HasPassword == true, "HasPassword is always true")
assert(user.HasConfiguredPassword == true, "HasConfiguredPassword is always true")
assert(user.HasConfiguredEasyPassword == false, "HasConfiguredEasyPassword is always false")
