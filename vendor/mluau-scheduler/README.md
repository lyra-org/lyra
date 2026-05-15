# mlua_scheduler

A Roblox-like scheduler for mlua

## Features

- Tokio support + minimal CPU usage
- Correctness and performance are king
- Simple with simple primitives for getting results out.
- Properly working ``coroutine.yield`` and ``coroutine.resume`` functions that produce equivalent (mostly) results in Roblox's own Luau + Task Scheduling code
- Both mlua non-send and send features supported (thanks to rust async wizardry).
- Custom async function handling that works with Lua's coroutine design paradigm without breaking on edge-cases.
- Simple callback API for controlling the scheduler (`SchedulerFeedback` trait)

## Deviations from Roblox's scheduler

- Thread arguments are not preloaded to the stack due to mlua limitations. This is normally not a problem but may cause side-effects. For example, see the below.

```lua
local task = task or require("@lune/task")
local thread = task.delay(1, function(foo)
    print(foo)
    print(coroutine.yield())
    print("c")
    print(coroutine.yield())
    print("e")
end, "b")

task.spawn(thread, "a")

task.delay(2, function()
    coroutine.resume(thread, "d")
end)
```

## Choose your scheduler

- **Rodan:** Scheduled tasks are stored in a ``tokio-util`` TaskTracker with ``spawn_local``'d tasks. This is the recommended scheduler for most use-cases due to its simplicity and decent performance.
- **Plinth:** Scheduled tasks are stored in a async task queue with all tasks polled at once. Unlike ``Rodan``, tasks are not immediately removed so cancellation etc. may be delayed and the overall scheduler is more complex. As such, this scheduler is recommended for advanced use-cases only.

## Archive Info

mlua_scheduler has two scheduler versions. v1 (the original scheduler which used busy poll loop/spinloop that ran every tick to run through tasks) and v2 which is a new scheduler design based on tokio select and delayqueue/tokio channels without any busy polling. Here are the differences:

mluau_scheduler no longer supports the v1 scheduler. The v2 scheduler is the only one supported and is the default.
- V2 scheduler is based on the Tokio runtime and uses its async features.
- V2 scheduler is more efficient and does not use busy polling.
- V2 scheduler is more robust and can handle more complex tasks with minimal performance loss to the v1 scheduler.
