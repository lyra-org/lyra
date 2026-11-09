local fixture = ...
local audio = require("../../routes/audio")
local item_ids = require("../../protocol/items/identifiers")

local ids = fixture.ids

local function request(method: string, id: string, body: string?, file_name: string?): any
	local item_id = item_ids.encode(id)
	local query: { [string]: { string } } = { api_key = { fixture.api_keys.phone } }
	if file_name then
		query.fileName = { file_name }
	end
	local ctx = {
		plugin_id = "jellyfin",
		request = {
			method = method,
			path = `/Audio/{item_id}/Lyrics`,
			headers = {},
			query = query,
			body_raw = body or "",
		},
		params = { itemId = item_id },
	} :: any
	if method == "GET" then
		return audio.handle_lyrics_get_route(ctx)
	elseif method == "POST" then
		return audio.handle_lyrics_post_route(ctx)
	end
	return audio.handle_lyrics_delete_route(ctx)
end

-- A song's best ranked lyrics, synced when the host counts them so.
local fetched = request("GET", ids.song)
assert(fetched.status == 200, `status {fetched.status}`)
assert(fetched.body.Metadata.IsSynced == true, "the provider's lyrics are synced")
assert(#fetched.body.Lyrics == 2 and fetched.body.Lyrics[2].Text == "Two")
assert(fetched.body.Lyrics[2].Start == 120000 * 10000, `start {fetched.body.Lyrics[2].Start}`)

assert(request("GET", ids.bare).status == 404, "a song without lyrics")

-- The caller's upload outranks the provider's; its few timed lines don't make it synced.
local uploaded = request("POST", ids.song, "[00:01.00]Mine\n[00:02.00]Too", "mine.lrc")
assert(uploaded.status == 200, `upload status {uploaded.status}`)
assert(uploaded.body.Metadata.IsSynced == false, "thin timing is unsynced")
local mine = request("GET", ids.song)
assert(mine.body.Lyrics[1].Text == "Mine" and mine.body.Metadata.IsSynced == false)
assert(mine.body.Lyrics[1].Start == nil and mine.body.Lyrics[1].Cues == nil, "no timings when unsynced")

assert(request("DELETE", ids.song).status == 204)
assert(request("GET", ids.song).body.Lyrics[1].Text == "One", "the provider's lyrics again")
