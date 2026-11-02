local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")

-- Editions without a date, so applying one asks for its release group's.
local by_group = refresh.release(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { country = "JP", artist_type = "Group" })
local single_track =
	refresh.release("single-track", refresh.JAPANESE_TITLES, { country = "JP", artist_type = "Group", track_count = 1 })
local STORED_OFFICIAL = {
	external_ids = { musicbrainz = { release_id = refresh.OFFICIAL, release_group_id = refresh.GROUP } },
}

local function group_of(edition: any): refresh.Route
	return { pattern = `/ws/2/release?release-group={refresh.GROUP}&`, body = { count = 1, releases = { edition } } }
end

local function assert_saved_before(release_id: string, enrichment_pattern: string)
	local ids = refresh.saved_ids.release
	assert(ids and ids.release_id == release_id, "the match is saved")
	local saved_at = refresh.first_event("save release")
	local enriched_at = refresh.first_event(enrichment_pattern)
	assert(enriched_at, `{enrichment_pattern} is asked for`)
	assert(saved_at and saved_at < enriched_at, "the match is saved before any enrichment is fetched")
end

-- A matched release keeps its match when its enrichment fails.
refresh.routes = {
	group_of(by_group),
	{ pattern = "/ws/2/release-group/", body = refresh.UNAVAILABLE },
	{ pattern = "/ws/2/artist/", body = refresh.UNAVAILABLE },
}
refresh.refresh(refresh.monitoring(STORED_OFFICIAL))
assert_saved_before(refresh.OFFICIAL, "/ws/2/release-group/")
assert(refresh.first_event("/ws/2/artist/"), "the artist's relations are asked for")
assert(refresh.saved_ids.t1 and refresh.saved_ids.t2, "both tracks are matched")
assert(next(refresh.unmatched) == nil, "nothing is unmatched")

-- Enrichment that succeeds adds to the saved match: the release group's date
-- and Wikidata item, and the artist's relations.
refresh.routes = {
	group_of(by_group),
	{
		pattern = `/ws/2/release-group/{refresh.GROUP}?`,
		body = {
			["first-release-date"] = "2024-11-23",
			relations = { { type = "wikidata", url = { resource = "https://www.wikidata.org/wiki/Q42" } } },
		},
	},
	{
		pattern = `/ws/2/artist/{refresh.DECO27}?`,
		body = { id = refresh.DECO27, name = "DECO*27", type = "Group", relations = {} },
	},
}
refresh.refresh(refresh.monitoring(STORED_OFFICIAL))
assert_saved_before(refresh.OFFICIAL, "/ws/2/release-group/")
local release_fields = refresh.saved_fields.release
assert(release_fields.release_date == "2024-11-23", "the release group's date is added")
assert(release_fields.release_title == "モニタリング", "the match's fields stay")
assert(refresh.saved_ids.release.wikidata_qid == "Q42", "the Wikidata item is added")
local artist_saves = refresh.events_matching("save deco")
assert(#artist_saves == 2, "the artist is saved with its match and again with its relations")
assert(
	(refresh.first_event("/ws/2/artist/") :: number) < #refresh.events
		and refresh.events[#refresh.events] == "save deco",
	"the artist's relations are saved after they are fetched"
)

-- A one-track release fetches the work its recording performs after the
-- track's match is saved.
local work_relation = { ["target-type"] = "work", type = "performance", work = { id = "work" } }
refresh.routes = {
	{
		pattern = "/ws/2/release/single-track?",
		body = refresh.release(
			"single-track",
			refresh.JAPANESE_TITLES,
			{ country = "JP", date = "2024-11-23", track_count = 1, without_recording_relations = true }
		),
	},
	{ pattern = "/ws/2/recording/recording-1?", body = { relations = { work_relation } } },
}
refresh.refresh(refresh.monitoring({ single = true, manual_ids = { musicbrainz = { release_id = "single-track" } } }))
local track_saved_at = refresh.first_event("save t1")
local work_asked_at = refresh.first_event("/ws/2/recording/recording-1?")
assert(track_saved_at and work_asked_at and track_saved_at < work_asked_at, "the track is saved first")
assert(refresh.saved_ids.t1.work_id == "work", "the work is added")
assert(refresh.saved_ids.t1.recording_id == "recording-1", "the track's match stays")
assert(refresh.saved_fields.t1.track_title == "モニタリング", "the track's fields stay")

-- A release the user chose keeps its match when its enrichment fails, and the
-- local track it has no place for is still unmatched, first.
refresh.routes = {
	{ pattern = "/ws/2/release/single-track?", body = single_track },
	{ pattern = "/ws/2/release-group/", body = refresh.UNAVAILABLE },
	{ pattern = "/ws/2/artist/", body = refresh.UNAVAILABLE },
}
refresh.refresh(refresh.monitoring({ manual_ids = { musicbrainz = { release_id = "single-track" } } }))
assert_saved_before("single-track", "/ws/2/release-group/")
assert(refresh.unmatched.t2 and not refresh.unmatched.t1, "the track without a place is unmatched")
local unmatched_at = refresh.first_event("unmatch t2")
local enriched_at = refresh.first_event("/ws/2/release-group/")
assert(unmatched_at and enriched_at and unmatched_at < enriched_at, "the unmatch is written first")

-- Enrichment requests are given up when the refresh's enrichment deadline,
-- 50 seconds in, passes, so the refresh ends normally with its match saved.
local clock = musicbrainz_api.clock
local now = 0
musicbrainz_api.clock = function(): number
	return now
end
-- Refreshes the stored release, which reaches its enrichment `seconds` after
-- the refresh starts.
local function at_enrichment(seconds: number)
	now = 0
	refresh.routes = {
		{
			pattern = `/ws/2/release?release-group={refresh.GROUP}&`,
			body = function(_url: string): any
				now = seconds
				return { count = 1, releases = { by_group } }
			end,
		},
		{
			pattern = "/ws/2/release-group/",
			body = function(_url: string, options: any): any
				assert(options.timeout_ms == 5000, `the request gets the time left, got {options.timeout_ms}`)
				return refresh.TIMED_OUT
			end,
		},
		{ pattern = "/ws/2/artist/", body = refresh.TIMED_OUT },
	}
	refresh.refresh(refresh.monitoring(STORED_OFFICIAL))
end
at_enrichment(45)
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL, "the match is saved")
assert(#refresh.events_matching("/ws/2/release-group/") == 1, "the release group is asked for")
assert(#refresh.events_matching("/ws/2/artist/") == 1, "the artist's relations are asked for")
for url, timeout_ms in refresh.timeouts do
	if string.find(url, "/ws/2/artist/", 1, true) then
		assert(timeout_ms == 5000, "every enrichment request gets the time left")
	end
end
at_enrichment(50)
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL, "the match is saved")
assert(#refresh.events_matching("GET ") == 1, "no enrichment is asked for once the deadline passes")
musicbrainz_api.clock = clock
