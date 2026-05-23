local task = require("@harmony/task")

local observed = {}

local delayed = task.delay(0, function(foo, extra)
	table.insert(observed, `foo={foo}`)
	table.insert(observed, `extra={extra}`)
	table.insert(observed, `yield={coroutine.yield()}`)
	table.insert(observed, "done")
end, "b")

task.spawn(delayed, "a")
task.spawn(function()
	local ok, value = coroutine.resume(delayed, "d")
	table.insert(observed, `manual={ok}:{value}`)
end)

task.wait(0)

assert(table.concat(observed, ",") == "foo=b,extra=a,yield=d,done,manual=true:nil")
