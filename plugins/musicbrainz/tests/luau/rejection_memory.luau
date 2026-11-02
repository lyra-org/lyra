local local_release = require("../../resolution/local_release")
local musicbrainz_api = require("../../resolution/musicbrainz_api")
local refresh = require("./_refresh")
local resolve = require("../../resolution/resolve")

-- A release rejected against what was known then, like an artist credited
-- under another name than the local tag, is judged again once the artist
-- search shows it is the local artist.
local context = refresh.monitoring()
context.artists[1].artist_name = "デコ"
local official = refresh.release(refresh.OFFICIAL, refresh.JAPANESE_TITLES, { country = "JP", date = "2024-11-23" })
official.complete = true
refresh.routes = {}
table.clear(refresh.saved_ids)
local before_search = true
local after_search = false
local resumed, err = coroutine.resume(coroutine.create(function()
	musicbrainz_api.begin_refresh()
	local info = local_release.from_context(context)
	before_search = resolve.try_release(official, info)
	local searched = local_release.with_artist_search(info, { [refresh.DECO27] = true }, false)
	after_search = resolve.try_release(official, searched)
	musicbrainz_api.end_refresh()
end))
assert(resumed, tostring(err))
assert(not before_search, "the release crediting another name is rejected at first")
assert(after_search, "once the artist search names the credited artist, it is accepted")
assert(refresh.saved_ids.release.release_id == refresh.OFFICIAL)

-- Verdicts belong to the evidence they were reached on. An edition the alias's
-- recording ruled out is judged again when AcoustID verifies another of its
-- recordings, and a release whose longer closing track was no evidence before
-- its recording was verified is judged again after.
local function judged(release: any, local_context: any, verified: { { [string]: boolean } }): { boolean }
	local verdicts: { boolean } = {}
	local ok, run_err = coroutine.resume(coroutine.create(function()
		musicbrainz_api.begin_refresh()
		local info = local_release.from_context(local_context)
		for _, recordings in verified do
			table.insert(
				verdicts,
				resolve.try_release(release, local_release.with_verified_recordings(info, recordings))
			)
		end
		musicbrainz_api.end_refresh()
	end))
	assert(ok, tostring(run_err))
	return verdicts
end

local edition = refresh.release("edition", refresh.JAPANESE_TITLES, { country = "JP", date = "2024-11-23" })
edition.complete = true
local alias_then_acoustid =
	judged(edition, refresh.monitoring(), { { ["alias-recording"] = true }, { ["recording-1"] = true } })
assert(not alias_then_acoustid[1], "the alias's recording isn't on the edition")
assert(alias_then_acoustid[2], "the recording AcoustID verifies is")

local romaji = refresh.monitoring({ titles = { "Romaji", "Hajime", "Saigo" } })
local closing = refresh.release("closing", { "Romaji", "始め", "最後" }, { country = "JP", date = "2024-11-23" })
closing.media[1].tracks[1].length = 176800
closing.media[1].tracks[2].length = 176613 + 240000
closing.complete = true
table.clear(refresh.saved_ids)
local ok, run_err = coroutine.resume(coroutine.create(function()
	musicbrainz_api.begin_refresh()
	local info = local_release.from_context(romaji)
	assert(not resolve.try_release(closing, info), "unverified, the longer closing track is another song")
	local verified = local_release.with_verified_recordings(info, { ["recording-2"] = true })
	assert(resolve.try_release(closing, verified), "with its recording verified, it carries a hidden track")
	musicbrainz_api.end_refresh()
end))
assert(ok, tostring(run_err))
