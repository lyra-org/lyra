-- Runs the release refresh handler against scripted HTTP responses and records
-- what it sends, saves and unmatches.
local lookup = require("../../resolution/lookup")
local metadata = require("@lyra/metadata")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local serde = require("@harmony/serde")

local module = {}

module.GROUP = "d65df2c5-8f90-4cc5-ab69-e6a40fd77054"
module.OFFICIAL = "abc82aa3-74e2-4e61-a661-3d0493cebf8a"
module.PSEUDO = "ad3a92ef-1ca9-43e4-9f8b-2fa11fc01eb2"
module.DECO27 = "bd247ab3-c22d-4b5d-a6fd-437771cacbbe"

-- A route body answered with a 503.
module.UNAVAILABLE = table.freeze({})
-- A route body answered as a request that ran out of its `timeout_ms`.
module.TIMED_OUT = table.freeze({})

-- A body is answered as JSON; a function body is called with the URL and the
-- request options when the request is sent, and its result answered.
export type Route = { pattern: string, body: any }

-- Each request is answered by the first route whose pattern its URL contains.
module.routes = {} :: { Route }
-- What a refresh did, in order: `GET <url>`, `save <node>`, `unmatch <node>`.
module.events = {} :: { string }
-- Each saved node's ids, fields and credits by role, as of its last save.
module.saved_ids = {} :: { [string]: { [string]: string } }
module.saved_fields = {} :: { [string]: { [string]: any } }
module.saved_credits = {} :: { [string]: { [any]: { any } } }
module.unmatched = {} :: { [string]: boolean }
-- Each request's `timeout_ms`, by URL.
module.timeouts = {} :: { [string]: number }

musicbrainz_api.send = function(options: any): (boolean, any)
	table.insert(module.events, `GET {options.url}`)
	module.timeouts[options.url] = options.timeout_ms
	for _, route in module.routes do
		if string.find(options.url, route.pattern, 1, true) then
			local body = if type(route.body) == "function" then route.body(options.url, options) else route.body
			if body == module.UNAVAILABLE then
				return true, { success = false, status_code = 503, body = "", retries = 0 }
			end
			if body == module.TIMED_OUT then
				return true, { success = false, error_kind = "timeout", body = "", retries = 0 }
			end
			return true, { success = true, status_code = 200, body = serde.json.encode(body), retries = 0 }
		end
	end
	error(`unexpected request {options.url}`)
end

local release_handler: ((any) -> ())? = nil
local release_filter: ((any) -> boolean)? = nil
local function noop() end
local provider = setmetatable({
	refresh = function(_self: any, entity_type: any, handler: (any) -> (), filter: (any) -> boolean)
		if entity_type == metadata.EntityType.Release then
			release_handler = handler
			release_filter = filter
		end
	end,
	layer = function(_self: any, node_id: string): any
		local ids: { [string]: string } = {}
		local fields: { [string]: any } = {}
		local credits: { [any]: { any } } = {}
		return setmetatable({
			set_id = function(_layer: any, id_type: string, value: string)
				ids[id_type] = value
			end,
			set_field = function(_layer: any, field: string, value: any)
				fields[field] = value
			end,
			set_credits = function(_layer: any, role: any, role_credits: { any })
				credits[role] = role_credits
			end,
			save = function()
				table.insert(module.events, `save {node_id}`)
				module.saved_ids[node_id] = table.clone(ids)
				module.saved_fields[node_id] = table.clone(fields)
				module.saved_credits[node_id] = table.clone(credits)
			end,
		}, {
			__index = function()
				return noop
			end,
		})
	end,
	mark_unmatched = function(_self: any, node_id: string, _id_types: { string })
		table.insert(module.events, `unmatch {node_id}`)
		module.unmatched[node_id] = true
	end,
}, {
	__index = function()
		return noop
	end,
})
lookup.register_refresh(provider :: any)

-- The events whose text contains `pattern`.
function module.events_matching(pattern: string): { string }
	local matching = {}
	for _, event in module.events do
		if string.find(event, pattern, 1, true) then
			table.insert(matching, event)
		end
	end
	return matching
end

-- The position of the first event containing `pattern`, if any.
function module.first_event(pattern: string): number?
	for i, event in module.events do
		if string.find(event, pattern, 1, true) then
			return i
		end
	end
	return nil
end

-- Runs the release refresh handler on `ctx` on a fresh thread, as each
-- refresh handler gets, keeping what earlier refreshes recorded.
function module.run(ctx: any)
	local handler = release_handler
	assert(handler, "the release refresh handler is registered")
	local thread = coroutine.create(handler)
	local resumed, err = coroutine.resume(thread, ctx)
	assert(resumed, tostring(err))
	assert(coroutine.status(thread) == "dead", "the refresh handler ends")
end

-- Runs the release refresh handler on `ctx` with nothing recorded yet.
function module.refresh(ctx: any)
	table.clear(module.events)
	table.clear(module.saved_ids)
	table.clear(module.saved_fields)
	table.clear(module.saved_credits)
	table.clear(module.unmatched)
	table.clear(module.timeouts)
	module.run(ctx)
end

-- Whether a background sync refreshes `ctx`.
function module.syncs(ctx: any): boolean
	local filter = release_filter
	assert(filter, "the release sync filter is registered")
	return filter(ctx)
end

export type MonitoringOptions = {
	titles: { string }?,
	external_ids: any?,
	manual_ids: any?,
	lookup_hints: any?,
	options: any?,
	-- Only the first track, as a one-track single.
	single: boolean?,
}

-- The DECO*27 single モニタリング as a refresh context: two tracks, by
-- default with their file tags' titles.
function module.monitoring(options: MonitoringOptions?): any
	local resolved: MonitoringOptions = options or {}
	local titles = resolved.titles
		or { "モニタリング", "モニタリング", "モニタリング -Instrumental-" }
	local artists = { { id = "deco", artist_name = "DECO*27" } }
	return {
		id = "release",
		release_title = titles[1],
		lookup_hints = resolved.lookup_hints,
		artists = artists,
		tracks = if resolved.single
			then {
				{ id = "t1", track_title = titles[2], disc = 1, track = 1, duration_ms = 176800, artists = artists },
			}
			else {
				{ id = "t1", track_title = titles[2], disc = 1, track = 1, duration_ms = 176800, artists = artists },
				{ id = "t2", track_title = titles[3], disc = 1, track = 2, duration_ms = 176613, artists = artists },
			},
		library = { language = "jpn", country = "JP" },
		external_ids = resolved.external_ids,
		manual_ids = resolved.manual_ids,
		options = resolved.options,
	}
end

export type ReleaseOptions = {
	status: string?,
	country: string?,
	language: string?,
	date: string?,
	artist_type: string?,
	-- How many of the single's two tracks it lists.
	track_count: number?,
	-- Lists its recordings without their relations, as if not asked for.
	without_recording_relations: boolean?,
}

-- A MusicBrainz edition of the single in group `GROUP`, browsed or fetched
-- with every include.
function module.release(id: string, titles: { string }, options: ReleaseOptions?): any
	local resolved: ReleaseOptions = options or {}
	local tracks = {}
	for position = 1, resolved.track_count or 2 do
		table.insert(tracks, {
			id = `{id}-{position}`,
			title = titles[position + 1],
			position = position,
			length = if position == 1 then 176800 else 176613,
			recording = {
				id = `recording-{position}`,
				relations = if resolved.without_recording_relations then nil else {},
			},
		})
	end
	return {
		id = id,
		title = titles[1],
		status = resolved.status or "Official",
		country = resolved.country,
		date = resolved.date,
		["text-representation"] = { language = resolved.language or "jpn" },
		["artist-credit"] = {
			{
				name = "DECO*27",
				artist = { id = module.DECO27, name = "DECO*27", type = resolved.artist_type or "Person" },
			},
		},
		["release-group"] = { id = module.GROUP, ["primary-type"] = "Single" },
		media = { { position = 1, ["track-count"] = #tracks, tracks = tracks } },
	}
end

module.JAPANESE_TITLES = { "モニタリング", "モニタリング", "モニタリング -Instrumental-" }
module.ENGLISH_TITLES = { "Monitoring", "Monitoring", "Monitoring -Instrumental-" }

return module
