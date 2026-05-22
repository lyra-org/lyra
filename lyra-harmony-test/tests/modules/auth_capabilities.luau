local auth = require("@lyra/auth")

local capabilities = auth.capabilities()

assert(capabilities.enabled == false)
assert(capabilities.allow_default_login_when_disabled == true)
assert(capabilities.default_username == "default")
assert(auth.login == nil)
