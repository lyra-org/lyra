local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
		-- Lune check
        ok, task = pcall(require, "@lune/task")
		if not ok then 
			-- Try lute 
			ok, task = pcall(require, "@lute/task")
			if not ok then
				error("Could not find task library")
			end
		end
    end
end

local started = os.clock()

local amount = 1000000
local batches = 5
local per_batch = amount / batches

for current = 1, batches do
	local thread = coroutine.running()

	print(`Batch {current} / {batches}`)

	for i = 1, per_batch do
		--print("Spawning thread #" .. i)
		task.spawn(function()
			task.wait(0.1)
			--_TEST_ASYNC_WORK(0.1)
			if i == per_batch then
				print("Last thread in batch #" .. current)
				assert(coroutine.status(thread) == "suspended", `Thread {i} has status {coroutine.status(thread)}`)
				task.spawn(thread)
			end
		end)
	end

	coroutine.yield()
end
local took = os.clock() - started
print(`Spawned {amount} sleeping threads in {took}s`)

return -1
