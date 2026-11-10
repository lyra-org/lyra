local fixture, harness = ...
local auth = require("@lyra/auth")
local playlists = require("@lyra/playlists")
local tracks = require("@lyra/tracks")
local users = require("@lyra/users")

local call = require("./_call")(harness)
local cover_art = require("../../catalog/cover_art")
local dates = require("../../protocol/dates")
local response = require("../../protocol/response")
local songs = require("../../catalog/songs")

-- `getPlaylist` streams the document `ok` answers with the playlist whole, byte for byte, in every
-- format and whether or not it has entries to show.

local key = fixture.api_keys.owner_key

local function query(params: { [string]: string }): { [string]: { string } }
	local result = {}
	for name, value in params do
		result[name] = { value }
	end
	return result
end

local function whole(id: string, params: { [string]: string }): any
	return harness.plugin:call(function()
		assert(auth.resolve_auth(key), "authenticated")
		local playlist = playlists.get({ id })[id]
		local links = playlists.entries({ id })[id] or {}
		local track_ids = {}
		for _, link in links do
			table.insert(track_ids, link.track_id)
		end
		local found: { [string]: any } = if #track_ids > 0 then tracks.get(track_ids) else {}
		local entry_tracks = {}
		local duration_ms = 0
		for _, link in links do
			local track = found[link.track_id]
			if track and track.available then
				table.insert(entry_tracks, track)
				duration_ms += track.duration_ms or 0
			end
		end
		local created_at_ms = playlist.created_at_ms or 0
		local owner = users.get({ playlist.owner_id })[playlist.owner_id]
		local document: { [string]: any } = {
			id = playlist.id,
			name = playlist.name,
			comment = playlist.description,
			coverArt = cover_art.ids({ playlist.id })[playlist.id],
			owner = if owner then owner.username else nil,
			public = playlist.is_public == true,
			songCount = #entry_tracks,
			duration = dates.seconds(duration_ms),
			created = dates.iso8601(created_at_ms),
			changed = dates.iso8601(playlist.updated_at_ms or created_at_ms),
			readonly = playlist.owner_id ~= fixture.users.owner,
		}
		if #entry_tracks > 0 then
			document.entry = songs.list(entry_tracks)
		end
		return response.ok(query(params), { playlist = document })
	end)
end

local compared = 0
for _, playlist in { "long", "empty", "bare", "foreign" } do
	for _, format in { { f = "json" }, { f = "jsonp", callback = "handle" }, {} } :: { { [string]: string } } do
		local params: { [string]: string } = table.clone(format)
		params.apiKey = key
		params.id = fixture.ids[playlist]
		local streamed = call.raw("getPlaylist", params)
		assert(streamed.kind == "stream", `{playlist} answered {streamed.kind}`)
		params.v, params.c = "1.16.1", "test"
		local expected = whole(fixture.ids[playlist], params)
		assert(streamed.headers["content-type"] == expected.headers["content-type"], playlist)
		local body = call.body(streamed)
		assert(body == expected.body, `{playlist} {format.f or "xml"}\nstreamed\n{body}\nwhole\n{expected.body}`)
		compared += 1
	end
end
assert(compared == 12, `{compared} documents compared`)

local long = call.ok("getPlaylist", key, { id = fixture.ids.long }).playlist
assert(#long.entry == 514 and long.songCount == 514, `{#long.entry} entries, song count {long.songCount}`)
assert(long.duration == 510 + 180 + 200 + 180 + 1, `duration {long.duration}`)
assert(long.entry[1].id == fixture.ids.t1 and long.entry[501].id == fixture.ids.t501, "order holds across batches")
assert(long.entry[511].id == fixture.ids.a and long.entry[513].id == fixture.ids.a, "repeats stay")
assert(long.entry[514].id == fixture.ids.t510, "and the hidden track is gone")
assert(call.ok("getPlaylist", key, { id = fixture.ids.empty }).playlist.entry == nil, "an empty playlist has no entry")
