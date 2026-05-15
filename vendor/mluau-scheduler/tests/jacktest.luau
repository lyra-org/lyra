local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
        task = require("@lune/task")
    end
end

--local task = task or require("@lune/task")

local thread = task.spawn(function()
    print("1")

    if _TEST_ASYNC_WORK then
        print(_TEST_ASYNC_WORK(1))
    else
        print(task.wait(1))
    end
    print("4")
    print(coroutine.yield())
    print("6")
end)

print("2")
coroutine.resume(thread, "3")
print("5")
