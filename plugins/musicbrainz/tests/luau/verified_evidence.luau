local local_release = require("../../resolution/local_release")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")
local resolve = require("../../resolution/resolve")

local CREDIT = { { name = "DECO*27", artist = { id = refresh.DECO27, name = "DECO*27", type = "Person" } } }

-- A release of `titles`, each track on recording `recordings[i]`.
local function edition(
	id: string,
	title: string,
	language: string,
	country: string,
	titles: { string },
	recordings: { string }
): any
	local tracks = {}
	for position, track_title in titles do
		table.insert(tracks, {
			id = `{id}-{position}`,
			title = track_title,
			position = position,
			length = 200000,
			recording = { id = recordings[position], relations = {} },
		})
	end
	return {
		id = id,
		title = title,
		status = "Official",
		country = country,
		date = "2020-01-01",
		["text-representation"] = { language = language },
		["artist-credit"] = CREDIT,
		["release-group"] = { id = refresh.GROUP, ["primary-type"] = "Album" },
		media = { { position = 1, ["track-count"] = #tracks, tracks = tracks } },
	}
end

local function local_album(title: string, titles: { string }): any
	local artists = { { id = "deco", artist_name = "DECO*27" } }
	local tracks = {}
	for position, track_title in titles do
		table.insert(tracks, {
			id = `t{position}`,
			track_title = track_title,
			disc = 1,
			track = position,
			duration_ms = 200000,
			artists = artists,
		})
	end
	return {
		id = "release",
		release_title = title,
		artists = artists,
		tracks = tracks,
		library = { language = "jpn", country = "JP" },
	}
end

-- Walks `hit`'s group with `verified` recordings known, and returns the
-- release ID applied.
local function walk(hit: any, context: any, verified: { [string]: boolean }): string?
	table.clear(refresh.saved_ids)
	local resumed, err = coroutine.resume(coroutine.create(function()
		musicbrainz_api.begin_refresh()
		local info = local_release.with_verified_recordings(local_release.from_context(context), verified)
		resolve.try_release_group(hit, info, context.release_title, nil, true)
		musicbrainz_api.end_refresh()
	end))
	assert(resumed, tostring(err))
	local ids = refresh.saved_ids.release
	return ids and ids.release_id
end

local GROUP_PAGE = `/ws/2/release?release-group={refresh.GROUP}&inc=`

-- A localized edition stands in for its identity only if it, too, carries the
-- verified recording.
local english = { "One", "Two", "Three", "Four", "Five" }
local identity = edition("identity", "Album", "eng", "XW", english, { "r1", "r2", "r3", "r4", "r5" })
local localized = edition(
	"localized",
	"アルバム",
	"jpn",
	"JP",
	{ "一", "二", "三", "四", "五" },
	{ "r1", "r2", "r3", "r4", "r6" }
)
-- The browse lists the localization without its tracks, so only fetching it in
-- full shows what it carries.
local localized_listing = table.clone(localized)
localized_listing.media = nil
localized_listing["track-count"] = 5
refresh.routes = {
	{ pattern = GROUP_PAGE, body = { count = 2, releases = { identity } } },
	{
		pattern = `/ws/2/release?release-group={refresh.GROUP}&limit=`,
		body = { count = 2, releases = { identity, localized_listing } },
	},
	{ pattern = "/ws/2/release/localized?", body = localized },
}
assert(
	walk(identity, local_album("Album", english), { r5 = true }) == "identity",
	"a localization without the verified recording doesn't stand in"
)

-- Editions listing their tracks without the verified recording don't use up
-- the editions a group may try.
local others = {}
for index = 1, 3 do
	table.insert(
		others,
		edition(`other-{index}`, "Album", "jpn", "JP", english, { "x1", "x2", "x3", "x4", `x{index}` })
	)
end
local carrier = edition("carrier", "Album", "eng", "XW", english, { "r1", "r2", "r3", "r4", "r5" })
refresh.routes = {
	{ pattern = GROUP_PAGE, body = { count = 4, releases = { others[1], others[2], others[3], carrier } } },
}
assert(walk(others[1], local_album("Album", english), { r5 = true }) == "carrier", "the edition carrying it is reached")

-- A recording an alias names constrains only the alias walk. When the walk
-- finds no edition with it, a later search may settle on a release the files
-- accept on their own, even one the walk passed over.
local function single(id: string, recording: string): any
	return edition(id, "Song", "eng", "XW", { "Song" }, { recording })
end
local function listed(id: string): any
	return {
		id = id,
		title = "Song",
		status = "Official",
		country = "XW",
		date = "2020-01-01",
		["track-count"] = 1,
		["release-group"] = { id = refresh.GROUP },
	}
end
local stranger = single("stranger", "stranger-song")
stranger["artist-credit"] = { { name = "Someone", artist = { id = "someone", name = "Someone" } } }
stranger["release-group"] = { id = "stranger-group" }
refresh.routes = {
	{
		pattern = "/ws/2/artist?query=",
		body = { count = 1, artists = { { id = refresh.DECO27, name = "DECO*27", score = 100, country = "JP" } } },
	},
	-- The transliteration search, after the alias, finds the first edition.
	{ pattern = "%20AND%20recording%3A", body = { count = 1, releases = { single("e1", "other") } } },
	-- The title search finds only another artist's song, so search goes on.
	{ pattern = "query=release%3ASong&", body = { count = 1, releases = { stranger } } },
	{ pattern = "/ws/2/release?release-group=stranger-group&", body = { count = 1, releases = { stranger } } },
	{ pattern = "/ws/2/release?query=", body = { count = 0, releases = {} } },
	{
		pattern = "/ws/2/recording?query=",
		body = {
			count = 1,
			recordings = { { id = "named", title = "Song", releases = { listed("e1") } } },
		},
	},
	{ pattern = GROUP_PAGE, body = { count = 0, releases = {} } },
	{
		pattern = `/ws/2/release?release-group={refresh.GROUP}&limit=`,
		body = { count = 4, releases = { listed("e1"), listed("e2"), listed("e3"), listed("e4") } },
	},
	{ pattern = "/ws/2/release/e1?", body = single("e1", "other") },
	{ pattern = "/ws/2/release/e2?", body = single("e2", "other") },
	{ pattern = "/ws/2/release/e3?", body = single("e3", "other") },
	{ pattern = "/ws/2/release/e4?", body = single("e4", "named") },
}
refresh.refresh(local_album("Song", { "Song" }))
assert(refresh.first_event("%20AND%20recording%3A"), "the weaker search ran after the alias")
local applied = refresh.saved_ids.release
assert(applied and applied.release_id == "e1", "the later search's match applies")

-- A recording an alias names is a hypothesis: when the walk finds its release
-- wrong for the files, a later search may still find the right recording.
local function single_of(id: string, recording: string, length: number, group: string): any
	local release = single(id, recording)
	release.media[1].tracks[1].length = length
	release["release-group"] = { id = group }
	return release
end
local long_take = single_of("long-take", "long", 240000, "long-group")
local right_take = single_of("right-take", "short", 200000, "right-group")
local function hit(id: string, group: string): any
	local listed_hit = listed(id)
	listed_hit["release-group"] = { id = group }
	return listed_hit
end
refresh.routes = {
	{
		pattern = "/ws/2/artist?query=",
		body = { count = 1, artists = { { id = refresh.DECO27, name = "DECO*27", score = 100, country = "JP" } } },
	},
	{ pattern = "%20AND%20recording%3A", body = { count = 1, releases = { right_take } } },
	{ pattern = "query=release%3ASong&", body = { count = 1, releases = { stranger } } },
	{ pattern = "/ws/2/release?release-group=stranger-group&", body = { count = 1, releases = { stranger } } },
	{ pattern = "/ws/2/release?query=", body = { count = 0, releases = {} } },
	{
		pattern = "/ws/2/recording?query=",
		body = {
			count = 1,
			recordings = { { id = "long", title = "Song", releases = { hit("long-take", "long-group") } } },
		},
	},
	{ pattern = "/ws/2/release?release-group=long-group&", body = { count = 1, releases = { long_take } } },
	{ pattern = "/ws/2/release?release-group=right-group&", body = { count = 1, releases = { right_take } } },
}
refresh.refresh(local_album("Song", { "Song" }))
assert(refresh.first_event("/ws/2/release?release-group=long-group&"), "the alias walk ran")
assert(refresh.saved_ids.release.release_id == "right-take", "the right recording found later applies")
