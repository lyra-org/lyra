local fixture = ...
local auth = require("@lyra/auth")

-- Device login is off whole while authentication is, on behalf of another user included.
assert(auth.capabilities().enabled == false)

local begun = auth.begin_device_login({ code = "123456" })
assert(begun.status == "disabled" and begun.result == nil)
assert(auth.device_login_status("secret").status == "disabled")
assert(auth.resolve_auth(nil) ~= nil)
assert(auth.approve_device_login("ABCDEF").status == "disabled")
assert(auth.approve_device_login("ABCDEF", { user_id = fixture.users.listener }).status == "disabled")
assert(auth.claim_device_login("secret").status == "disabled")
