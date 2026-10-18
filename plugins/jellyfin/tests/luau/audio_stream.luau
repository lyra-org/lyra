local fixture = ...

local audio = require("../../routes/audio")
local item_ids = require("../../protocol/items/identifiers")

local ids = fixture.ids

local function request(handler, id: number, api_key: string?, query: { [string]: string }?)
	local params = { api_key = if api_key then { api_key } else nil }
	for key, value in query or {} do
		params[key] = { value }
	end
	return handler({
		plugin_id = "jellyfin",
		request = {
			method = "GET",
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

-- `/universal` requires auth and the caller's access to the track.
local universal = request(audio.handle_universal_route, ids.song)
assert(universal.status == 401, `anonymous universal status {universal.status}`)
local viewer_universal = request(audio.handle_universal_route, ids.song, fixture.api_keys.phone)
assert(viewer_universal.status == 404, `viewer universal status {viewer_universal.status}`)
local owner_universal = request(audio.handle_universal_route, ids.song, fixture.api_keys.desk, { static = "true" })
assert(owner_universal.kind == "stream_track", `owner universal kind {owner_universal.kind}`)
assert(owner_universal.options == nil or owner_universal.options.public == nil, "owner universal is not public")
