local fixture = ...

local audio = require("../../routes/audio")
local item_ids = require("../../protocol/items/identifiers")

local ids = fixture.ids

local function request(handler, id: number, api_key: string?, query: { [string]: string }?, method: string?)
	local params = { api_key = if api_key then { api_key } else nil }
	for key, value in query or {} do
		params[key] = { value }
	end
	return handler({
		plugin_id = "jellyfin",
		request = {
			method = method or "GET",
			path = `/Audio/{item_ids.encode_jellyfin_item_id(id)}/stream`,
			headers = {},
			query = params,
			body_raw = "",
		},
		params = { itemId = item_ids.encode_jellyfin_item_id(id) },
	} :: any)
end

local function assert_public_stream(response, label: string)
	assert(response.kind == "stream_track", `{label} kind {response.kind} status {response.status}`)
	assert(response.track_id == ids.song, `{label} track {response.track_id}`)
	assert(response.options and response.options.public == true, `{label} is not public`)
end

-- `/stream` is public, as upstream: any caller streams any track, whatever its credentials.
assert_public_stream(request(audio.handle_stream_route, ids.song, nil, { static = "true" }), "anonymous static")
assert_public_stream(request(audio.handle_stream_route, ids.song), "anonymous transcode")
assert_public_stream(request(audio.handle_stream_route, ids.song, fixture.api_keys.phone), "viewer")
assert_public_stream(request(audio.handle_stream_route, ids.song, "invalid"), "invalid key")
assert(request(audio.handle_stream_route, ids.album).status == 404, "a release is not a stream")

-- HEAD answers as GET does; the server leaves out the body.
assert_public_stream(request(audio.handle_stream_route, ids.song, nil, { static = "true" }, "HEAD"), "HEAD static")
assert_public_stream(request(audio.handle_stream_route, ids.song, nil, nil, "HEAD"), "HEAD transcode")
assert(request(audio.handle_stream_route, ids.album, nil, nil, "HEAD").status == 404, "HEAD of a release")

-- A valid token makes the stream act as its user, which the server counts transcodes against;
-- an invalid one leaves the caller as it was. Only the owner sees the song.
local playback = require("../../protocol/items/playback")
assert(playback.resolve_track_for_playback(ids.song) == nil, "the viewer, after an invalid key")
assert_public_stream(request(audio.handle_stream_route, ids.song, fixture.api_keys.desk), "owner")
assert(playback.resolve_track_for_playback(ids.song) ~= nil, "the owner, after the owner's key")

-- `/universal` requires auth and the caller's access to the track.
local universal = request(audio.handle_universal_route, ids.song)
assert(universal.status == 401, `anonymous universal status {universal.status}`)
local viewer_universal = request(audio.handle_universal_route, ids.song, fixture.api_keys.phone)
assert(viewer_universal.status == 404, `viewer universal status {viewer_universal.status}`)
local owner_universal = request(audio.handle_universal_route, ids.song, fixture.api_keys.desk, { static = "true" })
assert(owner_universal.kind == "stream_track", `owner universal kind {owner_universal.kind}`)
assert(owner_universal.options == nil or owner_universal.options.public == nil, "owner universal is not public")

-- A client that plays the source gets the file; the fixture's unknown bitrate counts as 40 Mbps.
local function universal_direct(query: { [string]: string }): boolean
	local response = request(audio.handle_universal_route, ids.song, fixture.api_keys.desk, query)
	assert(response.kind == "stream_track", `universal kind {response.kind} status {response.status}`)
	return response.options == nil
end
assert(universal_direct({ container = "ogg", maxStreamingBitrate = "50000000" }), "the client plays ogg")
assert(not universal_direct({ container = "mp3", maxStreamingBitrate = "50000000" }), "the client plays mp3")

-- Only the maximum streaming bitrate, or upstream's 8 Mbps default, limits direct play. The
-- fixture's file runs at 3 kbps.
assert(universal_direct({ container = "ogg", audioBitRate = "1000", maxStreamingBitrate = "3000" }), "audio bitrate")
assert(not universal_direct({ container = "ogg", maxStreamingBitrate = "2999" }), "over the maximum")
assert(universal_direct({ container = "ogg" }), "under the default maximum")
local resolve_source = playback.resolve_playback_source_for_track
playback.resolve_playback_source_for_track = function(song: any, include_entry: boolean?): any
	local source = resolve_source(song, include_entry)
	if source then
		source = table.clone(source)
		source.bitrate_bps = nil
	end
	return source
end
assert(not universal_direct({ container = "ogg" }), "an unknown bitrate counts as 40 Mbps, over the default")
assert(universal_direct({ container = "ogg", maxStreamingBitrate = "40000000" }), "40 Mbps allowed")
playback.resolve_playback_source_for_track = resolve_source
