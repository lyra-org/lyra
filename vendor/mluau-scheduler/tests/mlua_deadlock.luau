--!nocheck
local task = task or require("@lune/task")

if _TEST_ASYNC_WORK == nil then
	_TEST_ASYNC_WORK = function(n)
		task.wait(n)
	end
end

task.spawn(function() 
	print("LUA: Wait 3 second")
	task.wait(3)
	print("LUA: Doing _TEST_ASYNC_WORK(5)")
	_TEST_ASYNC_WORK(5)
    print("LUA: Done with _TEST_ASYNC_WORK(15)")
    _TEST_ASYNC_WORK(15)
    print("LUA: task.wait(10)")
    task.wait(10)
    print("LUA: All done")
	--print("LUA: Done with _TEST_ASYNC_WORK(1)")
	--print("LUA: Wait 1 second")
	--task.wait(1)
	--print("LUA: Doing _TEST_ASYNC_WORK(2)")
	--_TEST_ASYNC_WORK(2)
	--print("LUA: Done with _TEST_ASYNC_WORK(2)")

	--print("LUA: Wait 1 second")
	--task.wait(1)

    --print("LUA: Rust Wait 1 second")
    --coroutine.yield()

	print("All done")
end)

print("LUA[OUTER]: Wait 3 second")
task.wait(3)
print("LUA[OUTER]: Doing _TEST_ASYNC_WORK(10)")
_TEST_ASYNC_WORK(10)

print("LUA[OUTER]: task.wait(10)")
task.wait(10)

print("LUA[OUTER]: All done")