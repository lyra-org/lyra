local function myErrorFn(err: string)
    _TEST_ASYNC_WORK(0.1)
    error({MyError = err})
end

task.wait(0.1)
local a, err = pcall(myErrorFn, "Hello") 
_TEST_ASYNC_WORK(1)
print(err, typeof(err))
assert(a == false)
assert(typeof(err) == "table")

for i = 0, 100 do
    local a, err = pcall(_ERROR, i)
    print(a, err)
end
assert(a == false, "Expected false")
--_ERROR(10)

xpcall(function(...)
    print("XPCALL", ..., debug.info(1, "sl"))
end, _ERROR, 10)

return 1, 2, pcall(_ERROR, 10)
