local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
        task = require("@lune/task")
    end
end

local thread = task.delay(1, function(foo)
    print(foo)
    print(coroutine.yield())
    print("c")
    print(coroutine.yield())
    print("e")
end, "b")

task.spawn(thread, "a")

task.delay(2, function()
    coroutine.resume(thread, "d")
end)
