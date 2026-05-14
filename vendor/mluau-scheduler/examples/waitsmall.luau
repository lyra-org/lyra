local task = task
if task == nil then
    if zune then
        task = zune.task
    else 
        task = require("@lune/task")
    end
end

local c = os.clock()
for i = 1, 50 do
    print(task.wait(0.1))
end
print(os.clock() - c)