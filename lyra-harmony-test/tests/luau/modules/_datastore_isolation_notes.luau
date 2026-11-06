-- The `notes` plugin of datastore_isolation: exports that use its own stores.

local datastore = require("@lyra/datastore")
local task = require("@harmony/task")

local NAME = "shared"

local module = {}

-- Opened while the module loads, in the requiring plugin's call.
local opened_at_load = datastore.get_or_create(NAME)

function module.set(key: string, value: any)
	datastore.get_or_create(NAME):set(key, value)
end

function module.get(key: string): any
	return datastore.get_or_create(NAME):get(key)
end

function module.clear(): number
	return datastore.get_or_create(NAME):clear()
end

function module.get_through_load_handle(key: string): any
	return opened_at_load:get(key)
end

function module.get_through_load_handle_after_await(key: string): any
	task.wait(0)
	return opened_at_load:get(key)
end

function module.set_through_load_handle(key: string, value: any)
	opened_at_load:set(key, value)
end

function module.get_after_await(key: string): any
	task.wait(0)
	return module.get(key)
end

function module.get_in_spawned_thread(key: string): any
	local value
	local done = false
	task.spawn(function()
		value = module.get(key)
		done = true
	end)
	while not done do
		task.wait(0)
	end
	return value
end

-- Calls `callback` from this plugin's code.
function module.call(callback: () -> any): any
	return callback()
end

return module
