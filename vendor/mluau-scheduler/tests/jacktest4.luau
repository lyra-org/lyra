local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
        task = require("@lune/task")
    end
end

local yielded = false
local thread = task.spawn(function()
    while true do
        local f = coroutine.yield()
        yielded = true
        f()
        yielded = false
    end
end)

task.spawn(thread, function() end)
print(yielded)
task.spawn(thread, task.wait, 1)
print(yielded)
task.wait(1)
print(yielded)
