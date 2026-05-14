--!nocheck
--!nolint UnknownGlobal

-- for testing
assert(table.pack ~= nil, "table.pack is nil")

local task = task or require("@lune/task")

local currTime = os.clock()

print("Waiting for 3 seconds")

local x = task.wait(3)
print(x)
 

task.wait(3)

print("Time waited:", os.clock() - currTime)

currTime = os.clock()

task.wait(3)

print("Time waited:", os.clock() - currTime)

currTime = os.clock()

_TEST_ASYNC_WORK(3)

print("Time waited (TAW):", os.clock() - currTime)
