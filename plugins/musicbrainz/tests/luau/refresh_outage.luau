local lookup = require("../../resolution/lookup")
local metadata = require("@lyra/metadata")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local serde = require("@harmony/serde")

local DECO27 = "bd247ab3-c22d-4b5d-a6fd-437771cacbbe"

-- Each request is answered by the first route whose pattern its URL contains.
type Route = { pattern: string, status: number, body: any? }
local routes: { Route } = {}
local requests: { string } = {}
musicbrainz_api.send = function(options: any): (boolean, any)
	table.insert(requests, options.url)
	for _, route in routes do
		if string.find(options.url, route.pattern, 1, true) then
			local body = if route.body ~= nil then serde.json.encode(route.body) else ""
			return true, { success = route.status == 200, status_code = route.status, body = body, retries = 0 }
		end
	end
	error(`unexpected request {options.url}`)
end

local saved: { string } = {}
local unmatched: { string } = {}
local release_handler: ((any) -> ())? = nil
local function noop() end
local provider = setmetatable({
	refresh = function(_self: any, entity_type: any, handler: (any) -> (), _filter: any)
		if entity_type == metadata.EntityType.Release then
			release_handler = handler
		end
	end,
	layer = function(_self: any, node_id: string): any
		return setmetatable({
			save = function()
				table.insert(saved, node_id)
			end,
		}, {
			__index = function()
				return noop
			end,
		})
	end,
	mark_unmatched = function(_self: any, node_id: string, _id_types: { string })
		table.insert(unmatched, node_id)
	end,
}, {
	__index = function()
		return noop
	end,
})
lookup.register_refresh(provider :: any)

local function monitoring(external_ids: any?): any
	local artists = { { id = "deco", artist_name = "DECO*27" } }
	return {
		id = "release",
		release_title = "モニタリング",
		lookup_hints = { artist_name = "DECO27", release_title = "Monitoring", year = 2024 },
		artists = artists,
		tracks = {
			{
				id = "t1",
				track_title = "モニタリング",
				disc = 1,
				track = 1,
				duration_ms = 176800,
				artists = artists,
			},
			{
				id = "t2",
				track_title = "モニタリング -Instrumental-",
				disc = 1,
				track = 2,
				duration_ms = 176613,
				artists = artists,
			},
		},
		library = { language = "jpn", country = "JP" },
		external_ids = external_ids,
	}
end

-- Runs a refresh on a fresh thread, as each refresh handler gets.
local function refresh(ctx: any)
	table.clear(requests)
	table.clear(saved)
	table.clear(unmatched)
	local handler = release_handler
	assert(handler, "the release refresh handler is registered")
	local resumed, err = coroutine.resume(coroutine.create(handler), ctx)
	assert(resumed, tostring(err))
end

-- A failed title search sends no weaker query, such as the folder name's, and
-- leaves the release as it was.
routes = {
	{
		pattern = "/ws/2/artist?query=",
		status = 200,
		body = { count = 1, artists = { { id = DECO27, name = "DECO*27", score = 100, country = "JP" } } },
	},
	{ pattern = "/ws/2/release?query=", status = 503 },
}
refresh(monitoring())
assert(#requests == 2, `no query follows the failed search, sent {#requests}`)
assert(string.find(requests[2], "release%3A", 1, true), "the title search is the failed request")
assert(#saved == 0, "nothing is applied")
assert(#unmatched == 0, "nothing is unmatched")

-- A failed fetch of the stored release keeps it, without searching past it.
routes = { { pattern = "/ws/2/release/stored?", status = 503 } }
refresh(monitoring({ musicbrainz = { release_id = "stored" } }))
assert(#requests == 1, `nothing follows the failed fetch, sent {#requests}`)
assert(#saved == 0, "nothing is applied")
assert(#unmatched == 0, "the stored match is kept")

-- A failed release group browse is not retried without its details.
routes = { { pattern = "/ws/2/release?release-group=", status = 503 } }
table.clear(requests)
local resumed, err = coroutine.resume(coroutine.create(function()
	assert(musicbrainz_api.browse_release_group("group", false, 2) == nil, "a failed browse lists nothing")
end))
assert(resumed, tostring(err))
assert(#requests == 1, `the failed browse is the last request, sent {#requests}`)
