local refresh = require("./_refresh")

local DATE = "2024-11-23"
local INTERNATIONAL = "international"
local official = refresh.release(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { country = "JP", date = DATE })
local pseudo = refresh.release(
	refresh.PSEUDO,
	refresh.ENGLISH_TITLES,
	{ status = "Pseudo-Release", language = "eng", date = DATE }
)
local international = refresh.release(
	INTERNATIONAL,
	refresh.ENGLISH_TITLES,
	{ status = "Official", country = "XW", language = "eng", date = DATE }
)
local HINTS = { artist_name = "DECO27", release_title = "Monitoring", year = 2024 }

-- Refreshes the single as the server holds it once `release_id`, with
-- `titles`, is applied: the context carries the applied titles, not the tags.
-- Returns the release ID the refresh stores.
local function refresh_stored(release_id: string, titles: { string }, editions: { any }): string?
	refresh.routes = {
		{
			pattern = `/ws/2/release?release-group={refresh.GROUP}&`,
			body = { count = #editions, releases = editions },
		},
	}
	refresh.refresh(refresh.monitoring({
		titles = titles,
		lookup_hints = HINTS,
		external_ids = { ["musicbrainz:release"] = release_id, ["musicbrainz:release-group"] = refresh.GROUP },
	}))
	assert(#refresh.events_matching("GET ") == 1, "the group's browse lists the stored release")
	local ids = refresh.saved_ids.release
	return ids and ids.release_id
end

-- A release stored as the pseudo-release moves to the official edition in the
-- library's language, and stays there.
assert(
	refresh_stored(refresh.PSEUDO, refresh.ENGLISH_TITLES, { pseudo, official }) == refresh.OFFICIAL,
	"the pseudo-release heals to the official edition"
)
assert(
	refresh_stored(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { pseudo, official }) == refresh.OFFICIAL,
	"the official edition stays"
)

-- So does one stored as another official edition in another language.
assert(
	refresh_stored(INTERNATIONAL, refresh.ENGLISH_TITLES, { international, official }) == refresh.OFFICIAL,
	"an international edition heals to the official edition"
)

-- A stored edition the group's browse leaves out still competes with the
-- editions it lists.
refresh.routes = {
	{ pattern = `/ws/2/release?release-group={refresh.GROUP}&`, body = { count = 1, releases = { pseudo } } },
	{ pattern = `/ws/2/release/{refresh.OFFICIAL}?`, body = official },
}
refresh.refresh(refresh.monitoring({
	titles = refresh.JAPANESE_TITLES,
	lookup_hints = HINTS,
	external_ids = { ["musicbrainz:release"] = refresh.OFFICIAL, ["musicbrainz:release-group"] = refresh.GROUP },
}))
assert(refresh.first_event(`/ws/2/release/{refresh.OFFICIAL}?`), "the stored edition is fetched")
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL, "the stored edition left out of the browse stays")

-- So does one stored without its group, which the edition names.
local GROUP_BROWSE = `/ws/2/release?release-group={refresh.GROUP}&`
refresh.routes = {
	{ pattern = GROUP_BROWSE, body = { count = 1, releases = { pseudo } } },
	{ pattern = `/ws/2/release/{refresh.OFFICIAL}?`, body = official },
}
refresh.refresh(refresh.monitoring({
	titles = refresh.JAPANESE_TITLES,
	lookup_hints = HINTS,
	external_ids = { ["musicbrainz:release"] = refresh.OFFICIAL },
}))
assert(#refresh.events_matching(GROUP_BROWSE) == 1, "the group is browsed once")
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL, "the edition stored without its group stays")

-- A group whose browse lists nothing is not browsed again.
refresh.routes = {
	{ pattern = GROUP_BROWSE, body = { count = 0, releases = {} } },
	{ pattern = `/ws/2/release/{refresh.OFFICIAL}?`, body = official },
}
refresh.refresh(refresh.monitoring({
	titles = refresh.JAPANESE_TITLES,
	lookup_hints = HINTS,
	external_ids = { ["musicbrainz:release"] = refresh.OFFICIAL, ["musicbrainz:release-group"] = refresh.GROUP },
}))
assert(#refresh.events_matching(`release-group={refresh.GROUP}&inc=`) == 1, "the empty group is browsed once")
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL, "the stored edition stays")

-- A release marked unmatched is skipped by background sync and searched for
-- again on an explicit refresh.
local unmatched = refresh.monitoring({ unmatched = { "musicbrainz:release" } })
assert(not refresh.syncs(unmatched), "background sync skips the unmatched release")
refresh.routes = {
	{
		pattern = "/ws/2/artist?query=",
		body = { count = 1, artists = { { id = refresh.DECO27, name = "DECO*27", score = 100, country = "JP" } } },
	},
	{ pattern = "/ws/2/release?query=", body = { count = 0, releases = {} } },
	{ pattern = "/ws/2/recording?query=", body = { count = 0, recordings = {} } },
}
refresh.refresh(unmatched)
assert(#refresh.events_matching("/ws/2/release/?") == 0, "the empty ID is not fetched")
assert(refresh.first_event("/ws/2/artist?query=") == 1, "the refresh searches")
assert(refresh.unmatched.release, "the release stays unmatched when nothing is found")
