local _, harness = ...

local datastore = require("@lyra/datastore")
local notes = require("@notes")

local own = datastore.get_or_create("shared")

-- The same name in two plugins is two stores.
own:set("who", "test")
notes.set("who", "notes")
own:set("only_test", true)
notes.set("only_notes", true)
assert(own:get("who") == "test")
assert(notes.get("who") == "notes")
assert(own:get("only_notes") == nil)
assert(notes.get("only_test") == nil)

-- A dependency's code reaches its own stores however the call gets there.
assert(notes.get_through_load_handle("who") == "notes")
assert(notes.get_through_load_handle_after_await("who") == "notes")
assert(coroutine.wrap(notes.get_through_load_handle)("who") == "notes")
notes.set_through_load_handle("cached", true)
assert(notes.get("cached") == true)
assert(own:get("cached") == nil)
assert(notes.get_after_await("who") == "notes")
assert(notes.get_in_spawned_thread("who") == "notes")
assert(harness.plugin:call(notes.get, "who") == "notes")

-- A coroutine of the caller's reaches the dependency's stores through its export.
assert(coroutine.wrap(notes.get)("who") == "notes")

-- A function keeps its own plugin's stores when another plugin calls it.
assert(notes.call(function()
	return datastore.get_or_create("shared"):get("who")
end) == "test")

-- Clearing one plugin's store leaves the other's alone.
assert(notes.clear() == 3)
assert(notes.get("who") == nil)
assert(own:get("who") == "test")
notes.set("who", "notes")
assert(own:clear() == 2)
assert(notes.get("who") == "notes")
