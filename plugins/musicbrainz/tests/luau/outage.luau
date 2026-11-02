local acoustid = require("../../resolution/acoustid")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")

-- Runs `body` on a fresh thread, as each refresh handler gets, and returns
-- whether a request failed on it.
local function fails_on_thread(body: () -> ()): boolean
	local failed = false
	local resumed, err = coroutine.resume(coroutine.create(function()
		body()
		failed = musicbrainz_api.request_failed()
	end))
	assert(resumed, tostring(err))
	return failed
end

local function status(code: number, body: string): any
	return { success = code == 200, status_code = code, body = body, retries = 0 }
end

local function fails(ok: boolean, response: any): boolean
	return fails_on_thread(function()
		musicbrainz_api.response_object(ok, response)
	end)
end

assert(fails(false, "connection refused"), "a transport error is a failure")
assert(fails(true, status(503, "")), "a server error is a failure")
assert(fails(true, status(200, "not json")), "an unreadable body is a failure")
assert(not fails(true, status(404, "")), "a 404 is an answer")
assert(not fails(true, status(200, '{"count": 0, "artists": []}')), "an empty result is an answer")

-- Enrichment neither records a failure nor stops after one.
refresh.routes = { { pattern = "/ws/2/artist/", body = refresh.UNAVAILABLE } }
table.clear(refresh.events)
assert(not fails_on_thread(function()
	musicbrainz_api.enrichment_get("/artist/a?inc=artist-rels")
end), "a failed enrichment request is not recorded")
assert(
	fails_on_thread(function()
		musicbrainz_api.note_request_failure()
		musicbrainz_api.enrichment_get("/artist/a?inc=artist-rels")
	end),
	"the earlier failure stands"
)
assert(#refresh.events == 2, "enrichment is sent after a failure")

local DECO27_SEARCH = {
	pattern = "/ws/2/artist?query=",
	body = { count = 1, artists = { { id = refresh.DECO27, name = "DECO*27", score = 100, country = "JP" } } },
}
local HINTS = { artist_name = "DECO27", release_title = "Monitoring", year = 2024 }

-- A failed title search sends no weaker query, such as the folder name's, and
-- leaves the release as it was.
refresh.routes = { DECO27_SEARCH, { pattern = "/ws/2/release?query=", body = refresh.UNAVAILABLE } }
refresh.refresh(refresh.monitoring({ lookup_hints = HINTS }))
assert(#refresh.events == 2, `no query follows the failed search, sent {#refresh.events}`)
assert(string.find(refresh.events[2], "release%3A", 1, true), "the title search is the failed request")
assert(next(refresh.saved_ids) == nil, "nothing is applied")
assert(next(refresh.unmatched) == nil, "nothing is unmatched")

-- A failed fetch of the stored release keeps it, without searching past it.
refresh.routes = { { pattern = "/ws/2/release/stored?", body = refresh.UNAVAILABLE } }
refresh.refresh(refresh.monitoring({ external_ids = { musicbrainz = { release_id = "stored" } } }))
assert(#refresh.events == 1, `nothing follows the failed fetch, sent {#refresh.events}`)
assert(next(refresh.unmatched) == nil, "the stored match is kept")

-- A failed AcoustID lookup after a search that found nothing keeps every
-- match, including the track and artist IDs the search would unmatch.
acoustid.compute_fingerprint = function(_track_id: string): string?
	return "fingerprint"
end
local NOTHING_FOUND = {
	DECO27_SEARCH,
	{ pattern = "/ws/2/release?query=", body = { count = 0, releases = {} } },
	{ pattern = "/ws/2/recording?query=", body = { count = 0, recordings = {} } },
}
local function after_acoustid(answer: any)
	refresh.routes = table.clone(NOTHING_FOUND)
	table.insert(refresh.routes, { pattern = "api.acoustid.org", body = answer })
	refresh.refresh(refresh.monitoring({ options = { acoustid_fingerprint = true } }))
	assert(#refresh.events_matching("api.acoustid.org") == 1, "AcoustID is asked")
end
after_acoustid(refresh.UNAVAILABLE)
assert(next(refresh.unmatched) == nil, "nothing is unmatched")
after_acoustid({ status = "ok", results = {} })
for _, node_id in { "release", "t1", "t2", "deco" } do
	assert(refresh.unmatched[node_id], `{node_id} is unmatched when nothing failed`)
end

-- A group browse whose second page fails lists nothing, rather than a first
-- page that may leave out the edition that fits.
refresh.routes = {
	{
		pattern = "release-group=group&inc=",
		body = { count = 3, releases = { refresh.release("first", refresh.JAPANESE_TITLES) } },
	},
	{ pattern = "release-group=group&limit=", body = refresh.UNAVAILABLE },
}
table.clear(refresh.events)
local listed: any = false
assert(
	fails_on_thread(function()
		-- Not a fast browse, so the group's count asks for the second page.
		listed = musicbrainz_api.browse_release_group("group", false, 2)
	end),
	"the failure is recorded"
)
assert(listed == nil, "a partly failed browse lists nothing")
assert(#refresh.events == 2, "both pages are asked")
