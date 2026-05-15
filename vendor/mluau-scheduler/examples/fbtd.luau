local task = task or require("@lune/task");
local thread = task.delay(1, function()
    print("waited.. 1 second?");
    task.wait(10);
    print("waited.. 10 seconds?");
    coroutine.yield();
    print("never print");
  end)
  
  task.spawn(thread);
  