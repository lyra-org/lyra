local auth = require("@lyra/auth")

local invalid_token = "not-a-real-token"
assert(auth.resolve_auth(invalid_token) == nil)
assert(not pcall(auth.get_session_data, invalid_token))
assert(not pcall(auth.set_session_data, invalid_token, { preference = "compact" }))
assert(not pcall(auth.set_session_data, invalid_token, nil))
