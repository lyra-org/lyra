local local_release = require("../../resolution/local_release")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")
local resolve = require("../../resolution/resolve")

local DATE = "2024-11-23"

-- An edition as a group's browse lists it without its tracks, ranked by
-- `country`; fetched in full, it carries `titles`.
local function listed(id: string, country: string): any
	return {
		id = id,
		title = "モニタリング",
		status = "Official",
		country = country,
		date = DATE,
		["track-count"] = 2,
	}
end
local function full(id: string, country: string, titles: { string }): any
	local release = refresh.release(id, titles, { country = country, date = DATE })
	if titles[3] ~= refresh.JAPANESE_TITLES[3] then
		-- Another song than the file's, so another length too.
		release.media[1].tracks[2].length = 120000
	end
	return release
end
local WRONG_SONG = { "モニタリング", "モニタリング", "Another Song" }

-- Walks the group of `editions`, each fetched as `bodies` gives it, from a
-- search hit for the first, and returns the release ID applied.
local function walk(editions: { any }, bodies: { [string]: any }): string?
	refresh.routes = {
		{ pattern = `/ws/2/release?release-group={refresh.GROUP}&inc=`, body = { count = 0, releases = {} } },
		{
			pattern = `/ws/2/release?release-group={refresh.GROUP}&limit=`,
			body = { count = #editions, releases = editions },
		},
	}
	for id, body in bodies do
		table.insert(refresh.routes, { pattern = `/ws/2/release/{id}?`, body = body })
	end
	table.clear(refresh.events)
	table.clear(refresh.saved_ids)
	local hit = table.clone(editions[1])
	hit["release-group"] = { id = refresh.GROUP }
	local resumed, err = coroutine.resume(coroutine.create(function()
		musicbrainz_api.begin_refresh()
		resolve.try_release_group(
			hit,
			local_release.from_context(refresh.monitoring()),
			"モニタリング",
			nil,
			true
		)
		musicbrainz_api.end_refresh()
	end))
	assert(resumed, tostring(err))
	local ids = refresh.saved_ids.release
	return ids and ids.release_id
end

local function fetches(): number
	return #refresh.events_matching("/ws/2/release/")
end

-- The best edition the files reject gives way to the next best.
assert(walk({ listed("jp", "JP"), listed("xw", "XW") }, {
	jp = full("jp", "JP", WRONG_SONG),
	xw = full("xw", "XW", refresh.JAPANESE_TITLES),
}) == "xw", "the second edition applies after the first is rejected")

-- At most three editions are fetched and checked.
assert(walk({ listed("a", "JP"), listed("b", "XW"), listed("c", "US"), listed("d", "GB") }, {
	a = full("a", "JP", WRONG_SONG),
	b = full("b", "XW", WRONG_SONG),
	c = full("c", "US", WRONG_SONG),
	d = full("d", "GB", refresh.JAPANESE_TITLES),
}) == nil, "the fourth edition is past the cap")
assert(fetches() == 3, `three editions are fetched, got {fetches()}`)

-- A failed fetch ends the walk with nothing applied.
assert(walk({ listed("jp", "JP"), listed("xw", "XW") }, {
	jp = refresh.UNAVAILABLE,
	xw = full("xw", "XW", refresh.JAPANESE_TITLES),
}) == nil, "nothing applies after a failed request")
assert(fetches() == 1, `nothing is fetched after the failure, got {fetches()}`)

-- A recording the match rests on, like the one an alias search names, must be
-- on the edition applied: an edition of another recording ranked above it
-- gives way to one that carries it.
local other_version = full("jp", "JP", refresh.JAPANESE_TITLES)
for _, track in other_version.media[1].tracks do
	track.recording.id = `other-{track.recording.id}`
end
refresh.routes = {
	{ pattern = `/ws/2/release?release-group={refresh.GROUP}&inc=`, body = { count = 0, releases = {} } },
	{
		pattern = `/ws/2/release?release-group={refresh.GROUP}&limit=`,
		body = { count = 2, releases = { listed("jp", "JP"), listed("xw", "XW") } },
	},
	{ pattern = "/ws/2/release/jp?", body = other_version },
	{ pattern = "/ws/2/release/xw?", body = full("xw", "XW", refresh.JAPANESE_TITLES) },
}
table.clear(refresh.saved_ids)
local resumed, err = coroutine.resume(coroutine.create(function()
	musicbrainz_api.begin_refresh()
	local info = local_release.from_context(refresh.monitoring())
	local verified = local_release.with_verified_recordings(info, { ["recording-1"] = true })
	local hit = table.clone(listed("xw", "XW"))
	hit["release-group"] = { id = refresh.GROUP }
	resolve.try_release_group(hit, verified, "モニタリング", nil, true)
	musicbrainz_api.end_refresh()
end))
assert(resumed, tostring(err))
assert(refresh.saved_ids.release.release_id == "xw", "the edition carrying the verified recording applies")

-- The search hit kept as the anchor is tried first even when the group's
-- browse leaves it out; once the files reject it, the browsed editions follow.
local anchor_full = full("anchor", "JP", WRONG_SONG)
local deluxe = full("deluxe", "XW", refresh.JAPANESE_TITLES)
deluxe.title = "モニタリング (Deluxe)"
refresh.routes = {
	{ pattern = `/ws/2/release?release-group={refresh.GROUP}&inc=`, body = { count = 1, releases = { deluxe } } },
	{ pattern = "/ws/2/release/anchor?", body = anchor_full },
}
table.clear(refresh.events)
table.clear(refresh.saved_ids)
local anchored, anchor_err = coroutine.resume(coroutine.create(function()
	musicbrainz_api.begin_refresh()
	local hit = table.clone(listed("anchor", "JP"))
	hit["release-group"] = { id = refresh.GROUP }
	resolve.try_release_group(hit, local_release.from_context(refresh.monitoring()), "モニタリング", nil, true)
	musicbrainz_api.end_refresh()
end))
assert(anchored, tostring(anchor_err))
assert(refresh.first_event("/ws/2/release/anchor?"), "the anchor is tried first")
assert(refresh.saved_ids.release.release_id == "deluxe", "the browsed edition follows the rejected anchor")

-- Each edition the walk tries takes its localized stand-in from the whole
-- group, even one picked before it.
local release_group = require("../../resolution/release_group")
local japanese = full("japanese", "JP", refresh.JAPANESE_TITLES)
local english = full("english", "XW", refresh.ENGLISH_TITLES)
english.title = "Monitoring"
english["text-representation"] = { language = "eng" }
local picks = release_group.ranked_release_group_candidates(
	{ japanese, english },
	local_release.from_context(refresh.monitoring()),
	"モニタリング",
	nil,
	nil,
	false,
	3
)
assert(#picks == 2 and picks[1].identity.id == "japanese", "the Japanese edition ranks first")
assert(picks[2].identity.id == "english", "the English edition follows")
local stand_in = picks[2].localized
assert(stand_in and stand_in.id == "japanese", "it is localized from the whole group")
