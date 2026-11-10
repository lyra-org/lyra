local fixture = ...
local libraries = require("@lyra/libraries")

-- Without manage_users, a caller reads only its own.
local own = libraries.by_user({ fixture.users.listener })[fixture.users.listener]
assert(#own == 1 and own[1].id == fixture.libraries.music, "its own libraries")
assert(not pcall(libraries.by_user, { fixture.users.other }), "another user's are refused")
