local fixture = ...
local lyrics = require("@lyra/lyrics")

local ids = fixture.ids
local song = ids.song

local function ranked(options: lyrics.LyricsByTrackOptions?): { string }
	local list = {}
	for _, summary in lyrics.by_track({ song }, options)[song] do
		table.insert(list, summary.id)
	end
	return list
end

local function same(actual: { string }, expected: { string }, label: string)
	assert(#actual == #expected, `{label}: {#actual} entries`)
	for index, id in expected do
		assert(actual[index] == id, `{label}: entry {index} is {actual[index]}`)
	end
end

assert(lyrics.MAX_BODY_BYTES == 64 * 1024, `MAX_BODY_BYTES {lyrics.MAX_BODY_BYTES}`)

-- Entries rank shared before provider ones, then synced ones, then by provider position; other
-- users' personal entries are left out.
same(ranked(), { ids.shared, ids.second_synced, ids.first_plain, ids.third_french }, "default")
same(
	ranked({ language = "French" }),
	{ ids.shared, ids.third_french, ids.second_synced, ids.first_plain },
	"a language ranks its entries first"
)
same(ranked({ require_synced = true }), { ids.second_synced }, "only synced")
same(ranked({ provider_id = "first" }), { ids.first_plain }, "one provider")
same(ranked({ provider_id = "unknown" }), {}, "an unknown provider")

local summaries = lyrics.by_track({ song })[song]
local summary = summaries[2]
assert(summary.synced == true and summary.source == "provider" and summary.scope == "shared")
assert(summary.provider_id == "second" and summary.language == "eng")
assert(summary.provider_lyrics_id == "second-lyrics" and summary.id ~= summary.provider_lyrics_id)
assert((summary :: any).lines == nil and (summary :: any).plain_text == nil, "a summary has no content")
assert(summaries[1].synced == false and summaries[1].source == "manual" and summaries[1].provider_id == nil)
assert(summaries[1].provider_lyrics_id == nil, "manual lyrics have no provider lyrics id")

-- A track without lyrics has an empty list; hidden, missing and non-track ids are absent.
local by_track = lyrics.by_track({ song, ids.bare, ids.hidden, ids.album, "missing" })
assert(#by_track[ids.bare] == 0, "a track without lyrics")
assert(by_track[ids.hidden] == nil and by_track[ids.album] == nil and by_track.missing == nil)

local ok, err = pcall(lyrics.by_track, { song }, { limit = 1 } :: any)
assert(not ok and string.find(tostring(err), "unknown field", 1, true), tostring(err))
ok, err = pcall(lyrics.by_track, { song }, { language = "not a language" })
assert(not ok, "an invalid language raises")

-- `get` returns full entries; other users' personal entries and hidden tracks' are absent.
local infos = lyrics.get({ ids.second_synced, ids.shared, ids.others, ids.secret_words, song, "missing" })
local synced = infos[ids.second_synced]
assert(synced.id == ids.second_synced and synced.synced == true and #synced.lines == 2)
assert(synced.track_id == song, "an entry names its track")
assert(synced.lines[2].ts_ms == 120000 and synced.lines[2].text == "Two")
assert(infos[ids.shared].plain_text == "Shared words" and infos[ids.shared].scope == "shared")
assert(infos[ids.others] == nil, "another user's personal lyrics")
assert(infos[ids.secret_words] == nil, "a hidden track's lyrics")
assert(infos[song] == nil and infos.missing == nil, "ids of anything but lyrics")

-- The caller's personal lyrics rank first and keep their id when replaced.
local upload = { content_type = "text/plain", body = "My words", language = "eng" }
assert(lyrics.set_personal(ids.hidden, upload) == nil, "a hidden track")
assert(lyrics.set_personal(ids.album, upload) == nil, "not a track")
assert(lyrics.set_personal("missing", upload) == nil, "a missing track")
local mine = lyrics.set_personal(song, upload) :: lyrics.LyricsInfo
assert(mine.scope == "personal" and mine.source == "manual" and mine.plain_text == "My words")
assert(mine.track_id == song)
assert(ranked()[1] == mine.id, "personal lyrics rank first")
local replaced = lyrics.set_personal(song, { content_type = "text/plain", body = "New words" }) :: lyrics.LyricsInfo
assert(replaced.id == mine.id and replaced.plain_text == "New words", "a replacement keeps the id")
assert(lyrics.get({ mine.id })[mine.id].plain_text == "New words")

ok = pcall(lyrics.set_personal, song, { content_type = "image/png", body = "x" })
assert(not ok, "an unsupported content type raises")
ok =
	pcall(lyrics.set_personal, song, { content_type = "text/plain", body = string.rep("x", lyrics.MAX_BODY_BYTES + 1) })
assert(not ok, "a body over MAX_BODY_BYTES raises")
ok, err = pcall(lyrics.set_personal, song, { content_type = "text/plain", body = "x", lang = "eng" } :: any)
assert(not ok and string.find(tostring(err), "unknown field", 1, true), tostring(err))

-- The caller's own lyrics of a track they can no longer see are removed, but answer false.
assert(lyrics.remove_personal(ids.hidden) == false, "a hidden track")
assert(lyrics.remove_personal(song) == true)
assert(lyrics.remove_personal(song) == false, "nothing left to remove")
assert(ranked()[1] == ids.shared)

-- Removing every entry needs the manage_metadata permission.
ok, err = pcall(lyrics.remove_all, song)
assert(not ok and string.find(tostring(err), "ManageMetadata permission required", 1, true), tostring(err))
