--- Run this with MAX_THREADS=1

local ok, err = pcall(coroutine.create, function()
    print("Hello from thread 1")
end)

assert(not ok, "This should not succeed if the thread limit is set to 1")
print(ok, err)