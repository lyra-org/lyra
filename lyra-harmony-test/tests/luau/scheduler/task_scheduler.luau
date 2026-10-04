local task = require("@harmony/task")

local total = 0
local cancelled = false

task.spawn(function(value)
	total += value
end, 41)

local cancelled_thread = task.delay(0, function()
	cancelled = true
end)
task.cancel(cancelled_thread)

task.wait(0)
total += 1

assert(total == 42)
assert(cancelled == false)
