local task = require("@harmony/task")

local total = 0

task.spawn(function(value)
	total += value
end, 41)

task.wait(0)
total += 1

assert(total == 42)

