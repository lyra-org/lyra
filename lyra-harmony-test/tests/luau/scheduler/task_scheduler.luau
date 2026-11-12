local task = require("@harmony/task")

local total = 0
local cancelled = false

task.spawn(function(value)
	total += value
end, 41)

-- A cancelled thread is closed, so the code holding it can't run it either.
local function assert_closed(thread: thread)
	assert(coroutine.status(thread) == "dead")
	local ok, message = coroutine.resume(thread)
	assert(not ok and message == "cannot resume dead coroutine")
end

local cancelled_thread = task.delay(0, function()
	cancelled = true
end)
task.cancel(cancelled_thread)
assert_closed(cancelled_thread)

local waiting = task.spawn(function()
	task.wait(60)
	cancelled = true
end)

task.wait(0)
total += 1
task.cancel(waiting)
assert_closed(waiting)

-- A thread that cancels itself goes on to where it next yields.
local reached = false
local cancels_itself = task.spawn(function()
	task.cancel(coroutine.running())
	reached = true
	task.wait(0)
	cancelled = true
end)
task.wait(0)
assert(reached)
assert_closed(cancels_itself)

-- A coroutine resumed by hand while it waits, and cancelled there, can't be closed as it runs.
-- It runs on to where it next yields, and is closed there.
local ran_on = false
local resumed_by_hand = coroutine.create(function()
	task.wait(60)
	task.cancel(coroutine.running())
	ran_on = true
	pcall(task.wait, 0)
	cancelled = true
end)
coroutine.resume(resumed_by_hand)
local resumed = table.pack(coroutine.resume(resumed_by_hand))
assert(resumed.n == 1 and resumed[1] == true)
assert(ran_on)
assert_closed(resumed_by_hand)

-- The coroutine that cancelled the one that resumed it goes on, yielding and waiting as before.
local inner_done = false
local outer, inner
outer = coroutine.create(function()
	task.wait(60)
	inner = coroutine.create(function()
		task.cancel(outer)
		coroutine.yield()
		task.wait(0)
		inner_done = true
	end)
	coroutine.resume(inner)
	coroutine.resume(inner)
	pcall(coroutine.yield)
	cancelled = true
end)
coroutine.resume(outer)
assert(coroutine.resume(outer))
assert_closed(outer)
assert(coroutine.status(inner) == "suspended")
task.wait(0)
task.wait(0)
assert(inner_done)

task.wait(0)
assert(total == 42)
assert(cancelled == false)
