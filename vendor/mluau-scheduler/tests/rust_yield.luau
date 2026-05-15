--!nocheck
print("LUA: Wait 1 second")
task.wait(1)
print("LUA: Doing _TEST_ASYNC_WORK(10)")
_TEST_ASYNC_WORK(10)
print("LUA: Done with _TEST_ASYNC_WORK(1)")
print("LUA: Wait 1 second")
task.wait(1)

print("LUA: Doing _TEST_ASYNC_WORK(2)")
_TEST_ASYNC_WORK(2)

print("LUA: Done with _TEST_ASYNC_WORK(2)")
print("LUA: Wait 5 second")
task.wait(5)
print("All done")

return 1