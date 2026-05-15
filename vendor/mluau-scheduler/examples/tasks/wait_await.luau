local tasklib = task
local task = require("task")

local t = task.create(function()
    print(0)
    tasklib.wait(5)
    print(2)
    return 4
end)

print(1)
local t2 = task.await(t)
print(3)
print(t2)