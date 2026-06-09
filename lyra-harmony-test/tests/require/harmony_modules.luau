local crypt = require("@harmony/crypt")
local serde = require("@harmony/serde")
local json = serde.json

assert(json.encode({ answer = 42 }) == '{"answer":42}')
assert(#crypt.random(4) == 8)
