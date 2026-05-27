local crypt = require("@harmony/crypt")
local json = require("@harmony/json")

assert(json.encode({ answer = 42 }) == '{"answer":42}')
assert(#crypt.random(4) == 8)
