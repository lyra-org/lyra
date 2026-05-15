local mt = {}

print("thread:", coroutine.running())
function mt.__tostring()
    print("thread:", coroutine.running())
    return "__tostring succeeded"
end

local t = setmetatable({}, mt)
pcall(error, t)
print(t)
