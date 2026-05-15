local LuauSignal = require("signals")
local interval = require("interval")

local a: LuauSignal.Identity<number> = LuauSignal()

interval.setTimeout(function()
    a:fire(1)
end, 10000)

interval.setTimeout(function()
    a:fire(2)
end, 20000)

interval.setTimeout(function()
    a:fire(3)
end, 30000)

local b = a:wait()
assert(b == 1, "Expected 1, got " .. b)
print("Got b " .. b)

local c = a:wait()
assert(c == 2, "Expected 2, got " .. c)
print("Got c " .. c)

local d = a:wait()
assert(d == 3, "Expected 3, got " .. d)
print("Got d " .. d)
