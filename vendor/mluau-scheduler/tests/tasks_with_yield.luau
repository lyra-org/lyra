--!nocheck
-- Create a coroutine
local co = coroutine.create(function()
    print("LUA: Doing _TEST_ASYNC_WORK(5)")
    _TEST_ASYNC_WORK(5)
    print("LUA: Done with _TEST_ASYNC_WORK(5)")
end)

coroutine.resume(co)

print("LUA: Wait 5 second")
task.wait(5)
_TEST_ASYNC_WORK(2)

return -1