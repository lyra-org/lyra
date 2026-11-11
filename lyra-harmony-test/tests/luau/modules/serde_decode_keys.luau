local serde = require("@harmony/serde")

-- Object keys are bytes; a NUL byte is part of the key.
local json = serde.json.decode('{"a\\u0000b": 1, "a": 2}')
assert(json["a\0b"] == 1 and json.a == 2, "JSON keys")
assert(serde.json.decode(serde.json.encode({ ["c\0d"] = true }))["c\0d"] == true, "JSON round trip")

local yaml = serde.yaml.decode('"a\\0b": 1\na: 2\n')
assert(yaml["a\0b"] == 1 and yaml.a == 2, "YAML keys")
