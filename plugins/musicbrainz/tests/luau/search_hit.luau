local local_release = require("../../resolution/local_release")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")
local resolve = require("../../resolution/resolve")

local DATE = "2024-11-23"
local work_relation = { ["target-type"] = "work", type = "performance", work = { id = "work" } }

-- A search result lists its tracks, but not what applying it needs, such as
-- the works its recordings perform.
local hit =
	refresh.release(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { date = DATE, without_recording_relations = true })
local full = refresh.release(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { date = DATE })
full.media[1].tracks[1].recording.relations = { work_relation }

-- Its group's browse lists nothing, so the hit competes on its own.
refresh.routes = {
	{ pattern = `/ws/2/release?release-group={refresh.GROUP}&`, body = { count = 0, releases = {} } },
	{ pattern = `/ws/2/release/{refresh.OFFICIAL}?`, body = full },
}
table.clear(refresh.events)
table.clear(refresh.saved_ids)
local resumed, err = coroutine.resume(coroutine.create(function()
	musicbrainz_api.begin_refresh()
	local info = local_release.from_context(refresh.monitoring())
	assert(resolve.try_release_group(hit, info, "モニタリング", nil, true), "the hit applies")
	musicbrainz_api.end_refresh()
end))
assert(resumed, tostring(err))

local fetched_at = refresh.first_event(`/ws/2/release/{refresh.OFFICIAL}?`)
local saved_at = refresh.first_event("save release")
assert(fetched_at and saved_at and fetched_at < saved_at, "the hit is fetched in full before it is applied")
assert(refresh.saved_ids.t1.work_id == "work", "the full release is the one applied")
