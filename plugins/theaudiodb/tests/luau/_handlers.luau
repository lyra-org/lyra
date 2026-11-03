--!strict
-- Registers the plugin's handlers on a stand-in provider, runs them against the scripted
-- responses of `_audiodb`, and records what they save.
local metadata = require("@lyra/metadata")

local audiodb = require("./_audiodb")
local handlers = require("../../handlers")

local module = {}

module.serve = audiodb.serve

-- Each saved node's ids, as of its last save.
module.saved_ids = {} :: { [string]: { [string]: string } }

local covers: { [any]: (any) -> any } = {}
local refreshes: { [any]: (any) -> () } = {}
local function noop() end
local provider = setmetatable({
	cover = function(_self: any, entity_type: any, _config: any, handler: (any) -> any)
		covers[entity_type] = handler
	end,
	refresh = function(_self: any, entity_type: any, handler: (any) -> (), _filter: any)
		refreshes[entity_type] = handler
	end,
	layer = function(_self: any, node_id: string): any
		local ids: { [string]: string } = {}
		return setmetatable({
			set_id = function(_layer: any, id_type: string, value: string)
				ids[id_type] = value
			end,
			save = function()
				module.saved_ids[node_id] = table.clone(ids)
			end,
		}, {
			__index = function()
				return noop
			end,
		})
	end,
}, {
	__index = function()
		return noop
	end,
})
handlers.register(provider :: any)

-- The URL of the cover the artist cover handler picks for `ctx`, if any.
function module.artist_cover(ctx: any): string?
	local result = covers[metadata.EntityType.Artist](ctx)
	return if result then result.candidates[result.selected_index].url else nil
end

local function run(entity_type: any, ctx: any)
	table.clear(module.saved_ids)
	local handler = refreshes[entity_type]
	assert(handler, "the refresh handler is registered")
	local thread = coroutine.create(handler)
	local resumed, err = coroutine.resume(thread, ctx)
	assert(resumed, tostring(err))
end

-- Runs the artist refresh handler on `ctx` with nothing saved yet.
function module.refresh_artist(ctx: any)
	run(metadata.EntityType.Artist, ctx)
end

-- Runs the release refresh handler on `ctx` with nothing saved yet.
function module.refresh_release(ctx: any)
	run(metadata.EntityType.Release, ctx)
end

return module
