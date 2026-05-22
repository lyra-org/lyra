local task = require("@harmony/task")

local observed = {}

local thread = task.delay(0.01, function(foo)
	table.insert(observed, "foo=" .. tostring(foo))
	table.insert(observed, "yield1=" .. tostring(coroutine.yield()))
	table.insert(observed, "after-yield1")
	table.insert(observed, "yield2=" .. tostring(coroutine.yield()))
	table.insert(observed, "after-yield2")
end, "b")

task.spawn(thread, "a")

task.delay(0.02, function()
	local ok, value = coroutine.resume(thread, "d")
	table.insert(observed, "manual=" .. tostring(ok) .. ":" .. tostring(value))
end)

task.wait(0.05)

error(table.concat(observed, ","))
