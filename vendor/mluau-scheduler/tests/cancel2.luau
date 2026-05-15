local th = task.delay(4, function()
    assert(false, "This code should not run")
    print("10 seconds have passed")
end)

task.delay(1, function()
    print("Cancelling task")
    task.cancel(th)
end)
