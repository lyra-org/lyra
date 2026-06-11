local auth = require("@lyra/auth")

local capabilities = auth.capabilities()

assert(capabilities.enabled == true)
assert(capabilities.allow_default_login_when_disabled == true)
assert(capabilities.default_username == "default")

assert(auth.resolve_auth(nil) == nil)
assert(auth.resolve_auth("not-a-real-token") == nil)

assert(auth.login("", nil) == nil)
assert(auth.login("missing-user", "wrong-password") == nil)

assert(auth.logout_session(nil) == false)
assert(auth.logout_session("not-a-real-token") == false)
