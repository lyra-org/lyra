local fixture, harness = ...
local call = require("./_call")(harness)
local serde = require("@harmony/serde")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function raw_decision(media_id: string, info: any, params: { [string]: string }?): any
	local query: { [string]: string } = { apiKey = key, f = "json", mediaId = media_id, mediaType = "song" }
	for name, value in params or {} do
		query[name] = value
	end
	local body = if type(info) == "string" then info else serde.json.encode(info)
	return call.raw("getTranscodeDecision", query, {
		http_method = "POST",
		headers = { ["content-type"] = "application/json" },
		body = body,
	})
end

local function decision_response(media_id: string, info: any, params: { [string]: string }?): any
	return (serde.json.decode(call.body(raw_decision(media_id, info, params))) :: any)["subsonic-response"]
end

local function decide(media_id: string, info: any): any
	local body = decision_response(media_id, info)
	assert(body.status == "ok", `decision failed: {body.error and body.error.message}: {serde.json.encode(info)}`)
	return body.transcodeDecision
end

local function decision_error(media_id: string, info: any, params: { [string]: string }?): (number, string)
	local body = decision_response(media_id, info, params)
	assert(body.status == "failed", `decision succeeded: {serde.json.encode(info)}`)
	return body.error.code, body.error.message
end

local FLAC = { containers = { "flac" }, audioCodecs = { "flac" }, protocols = { "http" } }
local MP3 = { containers = { "mp3" }, audioCodecs = { "mp3" }, protocols = { "http" } }
local TO_MP3 = { container = "mp3", audioCodec = "mp3", protocol = "http" }
local TO_FLAC = { container = "flac", audioCodec = "flac", protocol = "http" }

-- The fields `transcodeParams` hold for Hires's source and Lossy's.
local HIRES = "flac.flac.1613000.96000.2.24"
local LOSSY = "mp3.mp3.127000.44100.2."

local function codec_profile(codec: string, limitations: { any }): { any }
	return { { type = "AudioCodec", name = codec, limitations = limitations } }
end

local function required(name: string, comparison: string, values: { string }): any
	return { name = name, comparison = comparison, values = values, required = true }
end

local function optional(name: string, comparison: string, values: { string }): any
	return { name = name, comparison = comparison, values = values, required = false }
end

-- Clients learn of the extension without signing in.
local extensions = call.raw("getOpenSubsonicExtensions", { f = "json" }) :: any
local versions = {}
for _, extension in (serde.json.decode(extensions.body) :: any)["subsonic-response"].openSubsonicExtensions do
	versions[extension.name] = extension.versions[1]
end
assert(versions.transcoding == 1, "transcoding is advertised")

-- A source a profile takes is played directly, with its details; the reasons of profiles tried
-- before it don't matter then.
local direct = decide(ids.hires, { name = "Test", platform = "Test", directPlayProfiles = { MP3, FLAC } })
assert(direct.canDirectPlay == true and direct.canTranscode == false, "direct play")
assert(direct.transcodeReason == nil and direct.transcodeStream == nil, "nothing to transcode")
assert(direct.transcodeParams == `d.{HIRES}`, direct.transcodeParams)
local source = direct.sourceStream
assert(source.protocol == "http" and source.container == "flac" and source.codec == "flac", "source format")
assert(source.audioSamplerate == 96000 and source.audioBitdepth == 24 and source.audioChannels == 2, "source audio")
assert(source.audioBitrate == 1613000, `source bitrate {source.audioBitrate}`)

-- A cue sheet's span of a file plays directly in the file's format, as Lyra cuts it.
local span = decide(ids.span, { maxAudioBitrate = 2000000, directPlayProfiles = { FLAC } })
assert(span.canDirectPlay and span.transcodeParams == `d.{HIRES}`, "a span")

-- Empty lists take anything, and names match in any case and by their aliases.
assert(
	decide(ids.hires, { directPlayProfiles = { { containers = {}, audioCodecs = {}, protocols = {} } } }).canDirectPlay
)
assert(
	decide(ids.opus, { directPlayProfiles = { { containers = { "OGG" }, audioCodecs = { "Opus" } } } }).canDirectPlay
)
assert(
	decide(ids.opus, { directPlayProfiles = { { containers = { "opus" }, audioCodecs = { "opus" } } } }).canDirectPlay
)
local mp4 = { directPlayProfiles = { { containers = { "mp4" }, audioCodecs = { "flac" } } } }
assert(not decide(ids.hires, mp4).canDirectPlay, "a FLAC file isn't an MP4 one")

-- Each direct play profile that doesn't fit gives one reason, in order. Names Lyra doesn't know
-- match nothing.
local refused = decide(ids.hires, {
	directPlayProfiles = {
		MP3,
		{ containers = { "flac" }, audioCodecs = { "flac" }, protocols = { "hls", "rtsp" } },
		{ containers = { "flac" }, audioCodecs = { "alac" } },
		{ containers = { "flac" }, audioCodecs = { "flac" }, maxAudioChannels = 1 },
		{ containers = { "flac" } },
	},
	codecProfiles = codec_profile("flac", {
		optional("audioSamplerate", "LessThanEqual", { "48000" }),
		required("audioBitdepth", "Equals", { "16" }),
	}),
})
call.same(refused.transcodeReason, {
	"ContainerNotSupported",
	"ProtocolNotSupported",
	"AudioCodecNotSupported",
	"AudioChannelsNotSupported",
	"AudioBitDepthNotSupported",
}, "reasons")
assert(refused.canDirectPlay == false and refused.canTranscode == false, "no transcoding profiles")
assert(refused.errorReason == "No direct play or transcoding profile fits the song", refused.errorReason)
assert(refused.transcodeParams == nil, "nothing to stream")

-- A limitation that isn't required never rules direct play out, even one Lyra can't check; a
-- required one Lyra can't check always does.
local function plays_directly(limitation: any): boolean
	return decide(ids.hires, { directPlayProfiles = { FLAC }, codecProfiles = codec_profile("flac", { limitation }) }).canDirectPlay
end
assert(plays_directly(optional("audioSamplerate", "LessThanEqual", { "48000" })), "optional")
assert(plays_directly(optional("loudness", "Equals", { "1" })), "an unknown optional limitation")
assert(not plays_directly(required("loudness", "Equals", { "1" })), "an unknown required limitation")
assert(not plays_directly(required("audioBitrate", "Between", { "1" })), "an unknown comparison")
assert(not plays_directly(required("audioBitrate", "LessThanEqual", { "high" })), "a value that isn't a number")
local unknown = decide(ids.hires, {
	directPlayProfiles = { FLAC },
	codecProfiles = codec_profile("flac", { required("loudness", "Equals", { "1" }) }),
})
call.same(unknown.transcodeReason, { "LimitationNotSupported" }, "an unknown limitation's reason")

-- A source over the client's bitrate isn't played directly.
local over = decide(ids.hires, { maxAudioBitrate = 320000, directPlayProfiles = { FLAC, MP3 } })
call.same(over.transcodeReason, { "ContainerBitrateExceedsLimit", "ContainerNotSupported" }, "over")

-- Transcoding profiles are tried in order; Lyra streams only over HTTP, so other ones are skipped.
local transcoded = decide(ids.hires, {
	maxTranscodingAudioBitrate = 256000,
	directPlayProfiles = { MP3 },
	transcodingProfiles = {
		{ container = "flac", audioCodec = "flac", protocol = "hls" },
		{ container = "flac", audioCodec = "flac", protocol = "rtsp" },
		TO_MP3,
		TO_FLAC,
	},
})
assert(transcoded.canDirectPlay == false and transcoded.canTranscode == true, "transcodes")
call.same(transcoded.transcodeReason, { "ContainerNotSupported" }, "one reason per profile")
local target = transcoded.transcodeStream
assert(target.protocol == "http" and target.container == "mp3" and target.codec == "mp3", "to mp3")
assert(target.audioBitrate == 256000 and target.audioChannels == 2, `mp3 at {target.audioBitrate}`)
assert(target.audioSamplerate == 48000, `the encoder's rate, not {target.audioSamplerate}`)
assert(target.audioBitdepth == nil, "lossy audio has no depth")
assert(transcoded.transcodeParams == `t.{HIRES}.mp3.mp3.256000...`, transcoded.transcodeParams)

-- A lossless source is encoded at Lyra's default bitrate unless limited, and a lossy one at its
-- own; `maxAudioBitrate` caps transcodes too.
local default = decide(ids.hires, { transcodingProfiles = { TO_MP3 } })
assert(default.transcodeStream.audioBitrate == 192000, `default {default.transcodeStream.audioBitrate}`)
assert(default.transcodeParams == `t.{HIRES}.mp3.mp3....`, default.transcodeParams)
assert(default.transcodeReason and #default.transcodeReason == 0, "no direct play profiles, no reasons")
local capped = decide(ids.hires, { maxAudioBitrate = 128000, transcodingProfiles = { TO_MP3 } })
assert(capped.transcodeStream.audioBitrate == 128000, "maxAudioBitrate caps a transcode")
local own =
	decide(ids.lossy, { transcodingProfiles = { { container = "ogg", audioCodec = "opus", protocol = "http" } } })
assert(own.transcodeStream.audioBitrate == 127000 and own.transcodeStream.audioSamplerate == 48000, "lossy")
assert(own.transcodeParams == `t.{LOSSY}.ogg.opus.127000...`, own.transcodeParams)

-- A required bitrate is chosen against the source, not Lyra's default encode.
local exact = decide(ids.hires, {
	transcodingProfiles = { TO_MP3 },
	codecProfiles = codec_profile("mp3", { required("audioBitrate", "Equals", { "256000" }) }),
})
assert(exact.canTranscode and exact.transcodeStream.audioBitrate == 256000, "an exact bitrate")

-- The target codec's limitations lower the output where they can, together: a lossless encode at
-- the greatest sample rate every limitation allows, and at 16 bits.
local lowered = decide(ids.hires, {
	transcodingProfiles = { { container = "flac", audioCodec = "flac", protocol = "http", maxAudioChannels = 1 } },
	codecProfiles = codec_profile("flac", {
		required("audioSamplerate", "Equals", { "44100", "48000" }),
		required("audioSamplerate", "LessThanEqual", { "45000" }),
		required("audioBitdepth", "LessThanEqual", { "20" }),
	}),
})
local flac = lowered.transcodeStream
assert(flac.container == "flac" and flac.codec == "flac", "to flac")
assert(flac.audioSamplerate == 44100 and flac.audioBitdepth == 16 and flac.audioChannels == 1, "lowered")
assert(flac.audioBitrate == nil, "a lossless encode has no known bitrate")
assert(lowered.transcodeParams == `t.{HIRES}.flac.flac..44100.1.16`, lowered.transcodeParams)

-- An optional limitation gives way where it would leave no value a required one, or Lyra, allows.
local yielded = decide(ids.hires, {
	transcodingProfiles = { TO_FLAC },
	codecProfiles = codec_profile("flac", {
		optional("audioSamplerate", "LessThanEqual", { "44100" }),
		required("audioSamplerate", "Equals", { "48000" }),
		optional("audioChannels", "LessThanEqual", { "0" }),
	}),
})
assert(yielded.transcodeStream.audioSamplerate == 48000, "the required rate")
assert(yielded.transcodeStream.audioChannels == 2, "no channels isn't a choice")

-- Each value is judged by what Lyra delivers for it, and the highest that fits is taken.
local function stream_of(codec: string, profile: any, limitations: { any }): any
	local result =
		decide(ids.hires, { transcodingProfiles = { profile }, codecProfiles = codec_profile(codec, limitations) })
	assert(result.canTranscode, `no {codec} transcode`)
	return result.transcodeStream, result.transcodeParams
end
local floor = stream_of("mp3", TO_MP3, {
	required("audioBitrate", "NotEquals", { "32000" }),
	optional("audioBitrate", "LessThanEqual", { "1000" }),
})
assert(floor.audioBitrate == 192000, `an optional bitrate under the encoder's floor gives way: {floor.audioBitrate}`)
local mono = stream_of("flac", TO_FLAC, {
	required("audioChannels", "NotEquals", { "2" }),
	required("audioBitdepth", "NotEquals", { "24" }),
})
assert(mono.audioChannels == 1 and mono.audioBitdepth == 16, "other values than the source's")
local rate = stream_of("mp3", TO_MP3, { required("audioSamplerate", "Equals", { "44100", "96000" }) })
assert(rate.audioSamplerate == 44100, `a rate the encoder delivers: {rate.audioSamplerate}`)
local high = stream_of("mp3", TO_MP3, { required("audioBitrate", "GreaterThanEqual", { "256000" }) })
assert(high.audioBitrate == 256000, `the bound, as no higher value is named: {high.audioBitrate}`)
local named = stream_of("mp3", TO_MP3, {
	required("audioBitrate", "GreaterThanEqual", { "256000" }),
	optional("audioBitrate", "NotEquals", { "128000", "320000" }),
	optional("audioBitrate", "LessThanEqual", { "320000" }),
})
assert(named.audioBitrate == 256000, `a named value an optional limitation excludes: {named.audioBitrate}`)
local highest, highest_params = stream_of("flac", TO_FLAC, {
	required("audioSamplerate", "Equals", { "44100", "48000", "88200" }),
	optional("audioSamplerate", "LessThanEqual", { "50000" }),
	optional("audioChannels", "LessThanEqual", { "1" }),
})
assert(highest.audioSamplerate == 48000 and highest.audioChannels == 1, "the highest value allowed")
assert(highest_params == `t.{HIRES}.flac.flac..48000.1.`, highest_params)
local bounded = stream_of("mp3", TO_MP3, { required("audioBitrate", "LessThanEqual", { "300000" }) })
assert(bounded.audioBitrate == 300000, `the bound itself: {bounded.audioBitrate}`)

-- Values the kept limitations rule out cost nothing, so an optional limitation listing many
-- bitrates can't use up the search before a required one is met.
local narrow = stream_of("mp3", TO_MP3, {
	required("audioChannels", "Equals", { "1" }),
	required("audioSamplerate", "Equals", { "8000" }),
	required("audioBitrate", "LessThanEqual", { "32000" }),
	optional("audioBitrate", "Equals", {
		"320000",
		"256000",
		"224000",
		"192000",
		"160000",
		"128000",
		"112000",
		"96000",
		"80000",
		"64000",
		"56000",
		"48000",
		"40000",
		"32000",
	}),
})
assert(narrow.audioChannels == 1 and narrow.audioSamplerate == 8000, "mono at 8 kHz")
assert(narrow.audioBitrate == 32000, `32 kbps: {narrow.audioBitrate}`)

-- Other bitrates than Lyra's default are tried, asked for explicitly, highest first.
local not_default, not_default_params =
	stream_of("mp3", TO_MP3, { required("audioBitrate", "NotEquals", { "192000" }) })
assert(not_default.audioBitrate == 320000, `a required limit on the default: {not_default.audioBitrate}`)
assert(not_default_params == `t.{HIRES}.mp3.mp3.320000...`, not_default_params)
local optional_default = stream_of("mp3", TO_MP3, { optional("audioBitrate", "NotEquals", { "192000" }) })
assert(optional_default.audioBitrate == 320000, `an optional limit on the default: {optional_default.audioBitrate}`)
local laddered = decide(ids.hires, {
	maxTranscodingAudioBitrate = 200000,
	transcodingProfiles = { TO_MP3 },
	codecProfiles = codec_profile("mp3", { required("audioBitrate", "NotEquals", { "200000", "192000" }) }),
})
assert(
	laddered.transcodeStream.audioBitrate == 160000,
	`the highest bitrate under the maximum: {laddered.transcodeStream.audioBitrate}`
)
local fixed = stream_of("mp3", TO_MP3, { required("audioBitrate", "Equals", { "192000" }) })
assert(fixed.audioBitrate == 192000, "the default still meets a limit naming it")

-- A WAV profile keeps the source's depth unless a limitation lowers it.
local wav =
	decide(ids.hires, { transcodingProfiles = { { container = "wav", audioCodec = "pcm", protocol = "http" } } })
assert(wav.transcodeStream.audioBitdepth == 24 and wav.transcodeStream.codec == "pcm", "24-bit wav")
assert(wav.transcodeParams == `t.{HIRES}.wav.pcm_s24le....`, wav.transcodeParams)

-- Profiles that can't be met are skipped: a lossy source isn't made lossless, a required
-- limitation Lyra can't raise a value to or can't check rules a profile out, and so does an
-- encoder's fixed sample rate.
local function skipped(media_id: string, info: any, label: string)
	local result = decide(media_id, info)
	assert(result.canTranscode == false and result.transcodeStream == nil, label)
end
skipped(ids.lossy, { transcodingProfiles = { TO_FLAC } }, "lossy to lossless")
skipped(ids.hires, {
	transcodingProfiles = { TO_MP3 },
	codecProfiles = codec_profile("mp3", { required("audioSamplerate", "GreaterThanEqual", { "192000" }) }),
}, "no upsampling")
skipped(ids.hires, {
	transcodingProfiles = { { container = "m4a", audioCodec = "aac", protocol = "http" } },
	codecProfiles = codec_profile("aac", { required("audioProfile", "Equals", { "LC" }) }),
}, "an AAC profile Lyra doesn't know")
skipped(ids.hires, {
	transcodingProfiles = { { container = "ogg", audioCodec = "opus", protocol = "http" } },
	codecProfiles = codec_profile("opus", { required("audioSamplerate", "LessThanEqual", { "44100" }) }),
}, "opus is always 48 kHz")
skipped(ids.hires, {
	transcodingProfiles = { TO_MP3 },
	codecProfiles = codec_profile("mp3", { required("audioSamplerate", "Equals", { "45000" }) }),
}, "mp3 has no 45 kHz")
skipped(
	ids.hires,
	{ transcodingProfiles = { { container = "mka", audioCodec = "ac3", protocol = "http" } } },
	"unknown"
)

-- An optional limitation Lyra can't check doesn't rule a profile out.
local unchecked = decide(ids.hires, {
	transcodingProfiles = { { container = "mp4", audioCodec = "aac", protocol = "http" } },
	codecProfiles = codec_profile("aac", { optional("audioProfile", "Equals", { "LC" }) }),
})
assert(unchecked.transcodeStream.container == "mp4" and unchecked.transcodeStream.codec == "aac", "to aac")
assert(unchecked.transcodeParams == `t.{HIRES}.m4a.aac....`, unchecked.transcodeParams)

-- A song without a file can't be played at all.
local fileless = decide(ids.fileless, { directPlayProfiles = { FLAC }, transcodingProfiles = { TO_MP3 } })
assert(fileless.canDirectPlay == false and fileless.canTranscode == false, "fileless")
assert(fileless.errorReason == "The song can't be streamed" and fileless.sourceStream == nil, fileless.errorReason)
call.same(fileless.transcodeReason, { "ContainerNotSupported" }, "fileless reasons")

-- XML repeats each reason as an element.
local xml = call.body(raw_decision(ids.hires, { directPlayProfiles = { MP3, MP3 } }, { f = "xml" }))
local _, count = string.gsub(xml, "<transcodeReason>ContainerNotSupported</transcodeReason>", "")
assert(count == 2, xml)
assert(string.find(xml, "<sourceStream ", 1, true), xml)

-- Requests that can't be decided fail as Subsonic errors.
local get = call.raw("getTranscodeDecision", { apiKey = key, f = "json", mediaId = ids.hires, mediaType = "song" })
assert((serde.json.decode(call.body(get)) :: any)["subsonic-response"].error.code == 0, "GET is refused")
assert(decision_error(ids.hires, {}, { mediaType = "podcast" }) == 70, "Lyra has no podcasts")
assert(decision_error(ids.hidden, {}) == 70, "a hidden song")
assert(decision_error("nothing", {}) == 70, "an unknown song")
assert(decision_error(ids.hires, {}, { mediaType = "video" }) == 0, "an unknown media type")
assert(decision_error(ids.hires, {}, { mediaId = "" }) == 10, "the song is required")

-- ClientInfo must be well formed JSON of the right shape, though it may leave anything out.
assert(decide(ids.hires, "{}").canDirectPlay == false, "an empty ClientInfo")
local function invalid(body: any, expected: string)
	local code, message = decision_error(ids.hires, body)
	assert(code == 0 and string.find(message, expected, 1, true), message)
end
invalid("", "must hold the client's ClientInfo")
invalid("{", "isn't valid JSON")
invalid("[]", "ClientInfo must be an object")
invalid('{"directPlayProfiles":{}}', "directPlayProfiles must be an array")
invalid('{"directPlayProfiles":[{}, null]}', "directPlayProfiles[2] must be an object")
invalid(
	'{"directPlayProfiles":[{"containers":["flac", null]}]}',
	"directPlayProfiles[1].containers[2] must be a string"
)
assert(decide(ids.hires, '{"maxAudioBitrate":null,"directPlayProfiles":[{"containers":null}]}').canDirectPlay)
invalid('{"directPlayProfiles":[[]]}', "directPlayProfiles[1] must be an object")
invalid('{"name":"{\\"}","directPlayProfiles":{ }}', "directPlayProfiles must be an array")
invalid('{"directPlayProfiles":[{"containers":{"":null}}]}', 'an object uses the empty key ""')
invalid('{"":null}', 'an object uses the empty key ""')
invalid('{"name":"x", "" : 1}', 'an object uses the empty key ""')
assert(decide(ids.hires, '{"name":"","platform":"\\"","directPlayProfiles":[{"containers":[""]}]}'), "empty strings")
invalid({ maxAudioBitrate = -1 }, "maxAudioBitrate must be a non-negative integer")
invalid({ directPlayProfiles = { { protocols = { 1 } } } }, "directPlayProfiles[1].protocols[1] must be a string")
invalid(
	{ codecProfiles = codec_profile("mp3", { { name = "audioBitrate", comparison = "Equals", values = { 1 } } }) },
	"codecProfiles[1].limitations[1].values[1] must be a string"
)

-- `getTranscodeStream` streams as the decision's parameters say.
local function stream(params: { [string]: string }): any
	local query: { [string]: string } = { apiKey = key, f = "json", mediaType = "song" }
	for name, value in params do
		query[name] = value
	end
	return call.raw("getTranscodeStream", query)
end

local played = stream({ mediaId = ids.hires, transcodeParams = direct.transcodeParams })
assert(played.kind == "stream_track" and played.track_id == ids.hires and played.options == nil, "the file as it is")
local resumed = stream({ mediaId = ids.hires, transcodeParams = direct.transcodeParams, offset = "2.5" })
assert(resumed.options.start_offset_ms == 2500 and resumed.options.format == nil, "from an offset")

local encoded = stream({ mediaId = ids.hires, transcodeParams = lowered.transcodeParams, offset = "0" })
local options = encoded.options
assert(encoded.kind == "stream_track" and options.format == "flac", `stream {encoded.kind}`)
call.same(options.preferred_codecs, { "flac" }, "codec")
assert(options.sample_rate_hz == 44100 and options.channels == 1 and options.bit_depth == 16, "caps")
assert(options.bitrate_bps == nil and options.start_offset_ms == nil, "no bitrate, no offset")
local mp3 = stream({ mediaId = ids.hires, transcodeParams = transcoded.transcodeParams }).options
assert(mp3.format == "mp3" and mp3.bitrate_bps == 256000 and mp3.sample_rate_hz == nil, "mp3 options")
local lossy = stream({ mediaId = ids.lossy, transcodeParams = own.transcodeParams }).options
assert(lossy.format == "ogg" and lossy.bitrate_bps == 127000, "the lossy source's bitrate")

-- Errors are bare HTTP statuses with a message.
local function stream_error(params: { [string]: string }): number
	local failed = stream(params)
	assert(failed.kind == "text", `answered {failed.kind}`)
	assert(string.find(failed.headers["content-type"], "^text/plain"), failed.headers["content-type"])
	assert(not string.find(failed.body, "subsonic-response", 1, true), failed.body)
	return failed.status
end
local direct_params = direct.transcodeParams
assert(stream_error({ mediaId = ids.hires }) == 400, "transcodeParams are required")
assert(stream_error({ transcodeParams = direct_params }) == 400, "the song is required")
for _, offset in { "soon", "-1", "1e308", "inf", "nan" } do
	assert(stream_error({ mediaId = ids.hires, transcodeParams = direct_params, offset = offset }) == 400, offset)
end
assert(stream_error({ mediaId = ids.hires, transcodeParams = direct_params, mediaType = "video" }) == 400, "type")
assert(stream_error({ mediaId = ids.hires, transcodeParams = direct_params, mediaType = "podcast" }) == 404, "podcast")
assert(stream_error({ mediaId = ids.hidden, transcodeParams = direct_params }) == 404, "a hidden song")
assert(stream_error({ mediaId = ids.fileless, transcodeParams = "d......" }) == 404, "a song without a file")
assert(stream_error({ apiKey = "not-a-key", mediaId = ids.hires, transcodeParams = direct_params }) == 401, "key")

-- Parameters decided for another source, or for this one as it was, or that Lyra didn't make, are
-- gone.
for _, params in
	{
		`d.{LOSSY}`,
		"d.flac.flac.1613000.48000.2.24",
		"d.flac.flac.1613000.96000.2.16",
		"d.flac.flac",
		`x.{HIRES}`,
		`d.{HIRES}.extra`,
		`t.{HIRES}.mp3.mp3`,
		`t.{HIRES}.bogus.mp3....`,
		`t.{HIRES}.mp3.ac3....`,
		`t.{HIRES}.mp3.mp3.0...`,
		`t.{HIRES}.mp3.mp3.-1...`,
		`t.{HIRES}.mp3.mp3.1e3...`,
		`t.{HIRES}.mp3.mp3.4294967296...`,
	}
do
	assert(stream_error({ mediaId = ids.hires, transcodeParams = params }) == 410, params)
end
assert(stream_error({ mediaId = ids.hires, transcodeParams = `d.{LOSSY}` }) == 410, "another song's source")
