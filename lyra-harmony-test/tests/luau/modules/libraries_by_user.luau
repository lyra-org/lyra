local fixture = ...
local libraries = require("@lyra/libraries")

local manager = fixture.users.manager
local listener = fixture.users.listener
local lonely = fixture.users.lonely

-- A user manager reads the libraries of users holding no permission it lacks, in the order they
-- were created, including ones it can't see itself. A user granted none has an empty list; a missing user is absent.
local found = libraries.by_user({ manager, listener, listener, lonely, "missing" })
assert(#found[manager] == 1 and found[manager][1].id == fixture.libraries.music, "its own")
assert(#found[listener] == 2, "the listener's two")
assert(found[listener][1].id == fixture.libraries.music and found[listener][1].name == "Music")
assert(found[listener][2].id == fixture.libraries.archive, "one the manager can't see")
assert(found[listener][1].path == nil, "paths need manage_libraries")
assert(found[lonely] ~= nil and #found[lonely] == 0, "granted none")
assert(found.missing == nil, "a missing user")
assert(next(libraries.by_user({})) == nil, "no ids")

-- An admin holds permissions the manager lacks.
local ok, err = pcall(libraries.by_user, { fixture.users.admin })
assert(not ok and string.find(tostring(err), "permissions you do not hold", 1, true), tostring(err))
