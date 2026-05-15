--!nocheck
task.defer(function() 
    print("LUA: Doing _TEST_ASYNC_WORK(5)")
    _TEST_ASYNC_WORK(5)
    print("LUA: Done with _TEST_ASYNC_WORK(5)")
    print("LUA: Wait 5 second")
    task.wait(5)
end)