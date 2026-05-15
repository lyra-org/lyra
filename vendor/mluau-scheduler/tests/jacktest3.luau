local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
        task = require("@lune/task")
    end
end

local thread = task.spawn(function()
    print("a")
    print(task.wait(1))
    print("d")
    print(coroutine.yield())
    print("f")
end)

print("b")
coroutine.resume(thread, "c")
print("e")
