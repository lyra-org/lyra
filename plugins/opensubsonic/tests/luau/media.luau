local fixture, harness = ...
local call = require("./_call")(harness)
local lyrics = require("@lyra/lyrics")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function stream(params: { [string]: string }): any
	local query: { [string]: string } = { apiKey = key, f = "json" }
	for name, value in params do
		query[name] = value
	end
	return call.raw("stream", query)
end

-- `stream` serves the file as it is, checking the caller's access.
local raw = stream({ id = ids.synced })
assert(raw.kind == "stream_track" and raw.track_id == ids.synced, `stream kind {raw.kind}`)
assert(raw.options == nil, "no options serves the file")
assert(stream({ id = ids.synced, format = "raw", maxBitRate = "0" }).options == nil, "raw and no limit")
assert(stream({ id = ids.synced, format = "raw", maxBitRate = "128", timeOffset = "5" }).options == nil, "raw wins")

local transcoded = stream({ id = ids.synced, format = "mp3", maxBitRate = "128", timeOffset = "1" })
assert(transcoded.kind == "stream_track", `transcode kind {transcoded.kind}`)
assert(transcoded.options.format == "mp3" and transcoded.options.bitrate_bps == 128000)
assert(transcoded.options.start_offset_ms == 1000 and transcoded.options.public == nil)
assert(stream({ id = ids.synced, format = "MP4" }).options.format == "m4a", "mp4 delivers m4a")
local capped = stream({ id = ids.synced, maxBitRate = "96" })
assert(capped.options.bitrate_bps == 96000 and capped.options.format == nil, "a bitrate cap alone")

-- `estimateContentLength` always reaches the server, which waits only for a transcode: even the
-- file as it is gets transcoded when it is a cue sheet's segment.
local estimated = stream({ id = ids.synced, estimateContentLength = "true" })
assert(estimated.options.wait_for_transcode == true and estimated.options.format == nil, "alone")
local raw_estimated = stream({ id = ids.synced, format = "raw", maxBitRate = "128", estimateContentLength = "true" })
assert(raw_estimated.options.wait_for_transcode == true and raw_estimated.options.bitrate_bps == nil, "raw")
local segment = stream({ id = ids.segment, estimateContentLength = "true" })
assert(segment.kind == "stream_track" and segment.options.wait_for_transcode == true, `segment {segment.kind}`)
local complete = stream({ id = ids.synced, format = "mp3", estimateContentLength = "true" })
assert(complete.options.format == "mp3" and complete.options.wait_for_transcode == true, "waits")
assert(transcoded.options.wait_for_transcode == nil, "streams as it encodes by default")
assert(stream({ id = ids.synced, estimateContentLength = "false" }).options == nil, "false asks for nothing")
assert(
	string.find(stream({ id = ids.synced, estimateContentLength = "maybe" }).body, '<error code="0"', 1, true),
	"a bad boolean"
)

-- `timeOffset` takes fractional seconds, to the millisecond.
assert(stream({ id = ids.synced, timeOffset = "1.2345" }).options.start_offset_ms == 1234, "a fractional offset")
assert(stream({ id = ids.synced, timeOffset = "0.5" }).options.start_offset_ms == 500, "under a second")

-- `stream` fails in XML whatever the format asked for, as its errors aren't media.
local function stream_error(params: { [string]: string }): number
	local failed = stream(params)
	assert(failed.kind == "text", `stream answered {failed.kind}`)
	assert(string.find(failed.headers["content-type"], "^text/xml"), failed.headers["content-type"])
	local code = string.match(failed.body, '<error code="(%d+)"')
	assert(code, failed.body)
	return tonumber(code) :: number
end

local refused = stream({ id = ids.synced, format = "bogus" })
assert(stream_error({ id = ids.synced, format = "bogus" }) == 0, "an unknown format")
assert(
	string.find(refused.body, "The song can't be streamed with these options", 1, true),
	"the host's error stays in the log"
)
assert(stream_error({}) == 10, "stream needs an id")
assert(stream_error({ id = ids.hidden }) == 70, "a hidden song isn't streamed")
assert(stream_error({ id = ids.album }) == 70, "an album isn't streamed")

-- A stream the host fails to serve answers with a Subsonic error in XML, never the host's, whose
-- message can name the file on disk.
local function fallback_error(served: any): (number, string)
	local fallback = served.fallback
	assert(fallback and fallback.kind == "text", `{served.kind} has no fallback`)
	assert(string.find(fallback.headers["content-type"], "^text/xml"), fallback.headers["content-type"])
	local code, message = string.match(fallback.body, '<error code="(%d+)" message="([^"]*)"')
	assert(code, fallback.body)
	return tonumber(code) :: number, message :: string
end
local fileless = stream({ id = ids.fileless })
assert(fileless.kind == "stream_track" and fileless.track_id == ids.fileless, "served without asking first")
local code, message = fallback_error(fileless)
assert(code == 0 and message == "The song can't be streamed", message)
local _, refused_options = fallback_error(stream({ id = ids.synced, format = "mp3" }))
assert(refused_options == "The song can't be streamed with these options", refused_options)
assert(stream_error({ id = ids.synced, timeOffset = "soon" }) == 0, "a bad offset")
assert(stream_error({ apiKey = "not-a-key", id = ids.synced }) == 44, "streaming needs a key")
assert(stream_error({ f = "jsonp", callback = "cb", id = ids.hidden }) == 70, "jsonp asks for nothing else")

-- Lyrics come as a structured entry each, in rank order; a song without any has none.
local none = call.ok("getLyricsBySongId", key, { id = ids.synced }).lyricsList
assert(type(none) == "table" and none.structuredLyrics == nil, "no lyrics")
local none_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.synced }).body
assert(string.find(none_xml, "<lyricsList/>", 1, true), none_xml)

lyrics.set_personal(ids.synced, {
	content_type = "application/lrc",
	body = "[00:01.00]First line\n[01:40.00]Second line\n",
	language = "eng",
})
lyrics.set_personal(ids.plain, { content_type = "text/plain", body = "One\nTwo", language = "eng" })

local synced = call.ok("getLyricsBySongId", key, { id = ids.synced }).lyricsList.structuredLyrics
assert(#synced == 1)
local entry = synced[1]
assert(entry.synced == true and entry.lang == "eng", `synced {entry.synced} lang {entry.lang}`)
assert(entry.displayTitle == "Synced" and entry.displayArtist == "Amy")
assert(#entry.line == 2 and entry.line[1].value == "First line" and entry.line[1].start == 1000)
assert(entry.line[2].start == 100000)

local synced_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.synced }).body
assert(string.find(synced_xml, '<line start="1000">First line</line>', 1, true), synced_xml)
local empty_xml = call.raw("getLyricsBySongId", { apiKey = key, id = ids.hidden }).body
assert(string.find(empty_xml, "<error ", 1, true), "a hidden song's lyrics fail in XML too")

local ranked = call.ok("getLyricsBySongId", key, { id = ids.plain }).lyricsList.structuredLyrics
assert(#ranked == 3, `{#ranked} entries`)
local plain = ranked[1]
assert(plain.synced == false, "plain lyrics aren't synced")
assert(plain.lang == "eng", "the caller's own lyrics come first")
assert(#plain.line == 2 and plain.line[1].value == "One" and plain.line[1].start == nil)
local french = ranked[2]
assert(french.lang == "fra" and #french.line == 2 and french.line[1].value == "Un")
local fetched = ranked[3]
assert(fetched.lang == "eng" and fetched.line[1].value == "Uno", "a second entry in a language")

-- Timed lines that cover too little of the song count as unsynced.
lyrics.set_personal(ids.synced, {
	content_type = "application/lrc",
	body = "[00:01.00]First line\n[00:02.50]Second line\n",
	language = "eng",
})
local thin = call.ok("getLyricsBySongId", key, { id = ids.synced }).lyricsList.structuredLyrics[1]
assert(thin.synced == false and thin.line[1].start == nil, "thin timing is unsynced")

assert(call.error_code("getLyricsBySongId", key, {}) == 10, "lyrics need an id")
assert(call.error_code("getLyricsBySongId", key, { id = ids.hidden }) == 70, "a hidden song's lyrics")

-- Clients learn about lyrics, form posts and stream offsets without signing in.
local extensions = call.raw("getOpenSubsonicExtensions", { f = "json" }) :: any
local serde = require("@harmony/serde")
local names = {}
for _, extension in (serde.json.decode(extensions.body) :: any)["subsonic-response"].openSubsonicExtensions do
	names[extension.name] = extension.versions[1]
end
assert(names.songLyrics == 1 and names.formPost == 1 and names.transcodeOffset == 1 and names.apiKeyAuthentication == 1)

-- `download` serves the file as it is to callers allowed to download, failing in XML as `stream`
-- does.
local downloader = fixture.api_keys.downloader_key
local function download(api_key: string, params: { [string]: string }): any
	local query: { [string]: string } = { apiKey = api_key, f = "json" }
	for name, value in params do
		query[name] = value
	end
	return call.raw("download", query)
end
local function download_error(api_key: string, params: { [string]: string }): number
	local failed = download(api_key, params)
	assert(failed.kind == "text", `download answered {failed.kind}`)
	assert(string.find(failed.headers["content-type"], "^text/xml"), failed.headers["content-type"])
	local code = string.match(failed.body, '<error code="(%d+)"')
	assert(code, failed.body)
	return tonumber(code) :: number
end

local downloaded = download(downloader, { id = ids.synced, format = "mp3", maxBitRate = "128" })
assert(downloaded.kind == "download_track" and downloaded.track_id == ids.synced, `download kind {downloaded.kind}`)
assert(downloaded.options == nil, "a download is never transcoded")
assert(download_error(key, { id = ids.synced }) == 50, "the listener may not download")
assert(download_error(downloader, {}) == 10, "download needs an id")
assert(download_error(downloader, { id = ids.hidden }) == 70, "a hidden song isn't downloaded")
assert(download_error(downloader, { id = ids.album }) == 70, "an album isn't downloaded")
local download_code, download_message = fallback_error(download(downloader, { id = ids.fileless }))
assert(download_code == 0 and download_message == "The song can't be downloaded", download_message)
assert(download_error("not-a-key", { id = ids.synced }) == 44, "downloading needs a key")

-- `getLyrics` finds a visible song by its exact title and, when given, an artist's name, ignoring
-- case, and answers its best ranked lyrics as text.
local function lyrics_of(params: { [string]: string }): any
	return call.ok("getLyrics", key, params).lyrics
end

local found = lyrics_of({ artist = "Amy", title = "Plain" })
assert(found.artist == "Amy" and found.title == "Plain" and found.value == "One\nTwo", `lyrics {found.value}`)
assert(lyrics_of({ artist = "amy", title = "PLAIN" }).value == "One\nTwo", "ignoring case")
local by_title = lyrics_of({ title = "Synced" })
assert(by_title.title == "Synced" and by_title.value == "First line\nSecond line", `timed lines {by_title.value}`)

local function nothing(params: { [string]: string }, label: string)
	local result = lyrics_of(params)
	assert(type(result) == "table" and next(result) == nil, label)
end
nothing({ artist = "Bob", title = "Plain" }, "another artist")
nothing({ artist = "Amy", title = "Pla" }, "a partial title")
nothing({ artist = "Amy" }, "no title")
nothing({}, "no parameters")
nothing({ title = "Hidden" }, "a hidden song")
nothing({ title = "Segment" }, "a song without lyrics")

-- Only the song's own artists count: not its other credits, nor its album's artists.
nothing({ artist = "Amy", title = "Home" }, "a composer isn't the artist")
nothing({ artist = "Amy", title = "Away" }, "an album artist isn't the song's")
local home = lyrics_of({ artist = "Bob", title = "Home" })
assert(home.artist == "Bob" and home.value == "Home lyrics", `home {home.artist}`)
assert(lyrics_of({ artist = "Bob", title = "Away" }).value == "Away lyrics", "the song's artist")
local duet = lyrics_of({ artist = "Amy", title = "Duet" })
assert(duet.artist == "Bob, Amy" and duet.value == "Duet lyrics", `duet {duet.artist}`)

-- Case folds beyond ASCII, for titles and artists alike.
local joga = lyrics_of({ artist = "BJÖRK", title = "JÓGA" })
assert(joga.artist == "Björk" and joga.title == "Jóga" and joga.value == "Jóga lyrics", `joga {joga.value}`)

-- Every same-titled song is a candidate, not only the first few: the one song with lyrics comes
-- last of 60 in name order.
for _, params in { { title = "Same" }, { artist = "amy", title = "SAME" } } do
	local same = lyrics_of(params)
	assert(same.title == "same" and same.value == "Last of the same", `same {same.value}`)
end

local lyrics_xml = call.raw("getLyrics", { apiKey = key, artist = "Amy", title = "Plain" }).body
assert(string.find(lyrics_xml, ">One\nTwo</lyrics>", 1, true), lyrics_xml)
assert(string.find(lyrics_xml, 'artist="Amy"', 1, true) and string.find(lyrics_xml, 'title="Plain"', 1, true))
local nothing_xml = call.raw("getLyrics", { apiKey = key, title = "Segment" }).body
assert(string.find(nothing_xml, "<lyrics/>", 1, true), nothing_xml)
