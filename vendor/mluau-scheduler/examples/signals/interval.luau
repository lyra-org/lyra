type TimeoutThread = {
    thread: thread,
    isDone: boolean,
    isCancelled: boolean
}

type IntervalThread = {
    intervalWaiter: thread,
    isWaiting: boolean,
    isCancelled: boolean
}


local timeouts: { [number]: TimeoutThread } = {}
local intervals: { [number]: IntervalThread } = {}

--- Implementation of setTimeout for the AntiRaid runtime.
---
--- Functions like JavaScript's setTimeout.
---
--- Should also work on Roblox as well
local function setTimeout(callback: (any) -> (), delay: number?, ...): number 
    local args = {...}

    if(typeof(delay) == "string") then
        -- Convert string to number
        delay = tonumber(delay)
    end

    -- Default delay is 0
    delay = (delay or 0)

    local tid = #timeouts + 1

    timeouts[tid] = {
        isCancelled = false,
        isDone = false
    } :: TimeoutThread;

    timeouts[tid].thread = task.delay((delay :: number)/1000, function() 
        timeouts[tid].isDone = true -- Mark as done
        if(not timeouts[tid] or timeouts[tid].isCancelled) then
            return
        end

        callback(args)
    end);

    return tid
end

--- Implementation of clearTimeout for the AntiRaid runtime.
---
--- Functions like JavaScript's clearTimeout.
---
--- Should also work on Roblox as well
local function clearTimeout(tid: number)
    local th = timeouts[tid]

    if(not th) then
        return
    end

    timeouts[tid].isCancelled = true

    if not timeouts[tid].isDone then
        task.cancel(th.thread)
    end
end

--- Implementation of setInterval for the AntiRaid runtime.
---
--- Functions like JavaScript's setInterval.
---
--- Should also work on Roblox as well
local function setInterval(callback: (any) -> (), delay: number?, ...): number    
    local args = {...}
    if(typeof(delay) == "string") then
        -- Convert string to number
        delay = tonumber(delay)
    end

    -- Default delay is 0
    delay = delay or 0

    local tid = #intervals + 1
    intervals[tid] = {
        isWaiting = false,
        isCancelled = false
    } :: IntervalThread; -- Ensure its set initially
    intervals[tid].intervalWaiter = task.spawn(function()
        while true do
            if(not intervals[tid] or intervals[tid].isCancelled) then
                return
            end

            intervals[tid].isWaiting = true
            task.wait((delay :: number)/1000);
            intervals[tid].isWaiting = false

            if(not intervals[tid] or intervals[tid].isCancelled) then
                return
            end

            callback(args)
        end
    end);

    return tid
end

--- Implementation of clearInterval for the AntiRaid runtime.
---
--- Functions like JavaScript's clearInterval.
---
--- Should also work on Roblox as well
local function clearInterval(tid: number)
    local th = intervals[tid]

    if(not th) then
        return
    end

    intervals[tid].isCancelled = true

    -- If isWaiting, remove the intervalWaiter from waiting queue
    if intervals[tid].isWaiting then
        task.cancel(intervals[tid].intervalWaiter)
    end
end

return {
    setTimeout = setTimeout,
    clearTimeout = clearTimeout,
    setInterval = setInterval,
    clearInterval = clearInterval
}