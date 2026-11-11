local serde = require("@harmony/serde")

-- Only YAML 1.2 Core spellings are booleans; YAML 1.1 words such as `no` stay strings.
local decoded = serde.yaml.decode("a: true\nb: False\nc: no\nd: yes\ne: on\nf: off\ng: y\n")
assert(decoded.a == true and decoded.b == false, "core booleans")
assert(decoded.c == "no" and decoded.d == "yes", "no and yes stay strings")
assert(decoded.e == "on" and decoded.f == "off" and decoded.g == "y", "on, off and y stay strings")
