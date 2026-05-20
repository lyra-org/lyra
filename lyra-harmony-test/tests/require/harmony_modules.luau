local crypt = require("@harmony/crypt")
local json = require("@harmony/json")
local task = require("@harmony/task")

assert(json.encode({ answer = 42 }) == '{"answer":42}')
assert(#crypt.random(4) == 8)
assert(type(task.spawn) == "function")

