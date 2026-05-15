local task = task or require("@lune/task")
task.spawn(function() 
    task.wait(2)
    print("ABC")
    print("_TEST_ASYNC_WORK(2)")
    _TEST_ASYNC_WORK(2)
    print("DEF")
    _TEST_ASYNC_WORK(2)
end)


print("CDE")