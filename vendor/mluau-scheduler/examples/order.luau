--!nocheck
--!nolint UnknownGlobal

-- for testing
assert(table.pack ~= nil, "table.pack is nil")

local task = task or require("@lune/task")

local threads: { [thread]: number } = {
	[coroutine.running()] = 1,
}

local threadIdReserved = 1

print("time", "", "thread", "", "info")

local function printDebug(...: string)
	local thread = coroutine.running()

	if threads[thread] then
		return print(string.format("%.3f", os.clock()), `thread {threads[thread]}`, ...)
	else
		threadIdReserved += 1
		threads[thread] = threadIdReserved
		return printDebug(...)
	end
end

task.defer(function()
	task.defer(function()
		printDebug("This was also deffered!")
	end)
	printDebug("This was deffered!")
end)

task.spawn(function()
	printDebug("Waiting for 1 second...")
	task.wait(1)
	printDebug("Finished waiting")
end)

local thread2 = task.spawn(function()
	printDebug("Waiting for 1 second...")
	task.wait(1)
	printDebug("Finished waiting")
end)

coroutine.close(thread2)

printDebug("Waiting for 0.5 seconds")

task.delay(2, function()
	printDebug("This was delayed")
end)

task.wait(0.5)

printDebug(`Canceled thread {threads[thread2]}`)
