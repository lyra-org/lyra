local fixture = ...
local item_ids = require("../../protocol/items/identifiers")
local playlist_access = require("../../protocol/playlist_access")
local session = require("../../protocol/session")

-- Creating a playlist or adding to one keeps the tracks the caller can see, once each in request
-- order, and drops the rest; replacing a playlist's tracks takes them exactly or fails.

local ids = fixture.ids
local function hex(id: string): string
	return item_ids.encode(id) :: string
end
local s1, s2, h1, album = hex(ids.s1), hex(ids.s2), hex(ids.h1), hex(ids.solo)
local nothing = string.rep("a", 32)
assert(item_ids.decode(nothing) ~= nil, "a well-formed id")

local function context(params: { [string]: string }, query: { [string]: { string } }, body: { [string]: any }?): any
	return {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/Playlists",
			headers = { authorization = `MediaBrowser Token="{fixture.api_keys.listener_key}"` },
			query = query,
			body_raw = "",
			json = body,
		},
		params = params,
	}
end

local function tracks_of(playlist_id: string): string
	local names = {}
	for _, track_id in playlist_access.track_ids(playlist_access.find(playlist_id) :: any) do
		local name = "?"
		for _, key in { "s1", "s2", "h1" } do
			if ids[key] == track_id then
				name = key
			end
		end
		table.insert(names, name)
	end
	return table.concat(names, ",")
end

local function create(requested: { string }): (number, string?, string?)
	local response = session.handle_create_playlist_route(context({}, {}, { Name = "New", Ids = requested })) :: any
	local id = if response.status == 200 then item_ids.decode(response.body.Id) else nil
	return response.status, if id then tracks_of(id) else nil, if response.body then response.body.Message else nil
end

local status, created = create({ s2, nothing, album, s1, s2, h1 })
assert(status == 200, `a create with some visible tracks succeeds, got {status}`)
assert(created == "s2,s1", `the visible tracks once each in order, got "{created}"`)
status, created = create({ album })
assert(status == 200, `a create naming no visible track succeeds, got {status}`)
assert(created == "", `and makes an empty playlist, got "{created}"`)

local mix = hex(ids.mix)
local function add(requested: string): (number, string?)
	local response =
		session.handle_add_playlist_items_route(context({ playlistId = mix }, { ids = { requested } })) :: any
	return response.status, if response.body then response.body.Message else nil
end

local added, message = add(`{s2},{nothing},{album},{h1},{s2}`)
assert(added == 204, `an add with a visible track succeeds, got {added}`)
assert(tracks_of(ids.mix) == "s1,s2", `the visible track is added once, got "{tracks_of(ids.mix)}"`)
for _, requested in { album, h1, nothing } do
	added, message = add(requested)
	assert(added == 400 and message == "missing ids query parameter", `no visible track, got {added} {message}`)
end
assert(tracks_of(ids.mix) == "s1,s2", "and nothing is added")

local function replace(requested: { string }): (number, string?)
	local response = session.handle_update_playlist_route(context({ playlistId = mix }, {}, { Ids = requested })) :: any
	return response.status, if response.body then response.body.Message else nil
end

local replaced
replaced, message = replace({ s2, "not-hex" })
assert(replaced == 400 and message == "invalid playlist item ids", `a malformed id, got {replaced} {message}`)
replaced, message = replace({ s2, s1, s1 })
assert(replaced == 204, `a replacement of visible tracks succeeds, got {replaced} {message}`)
assert(tracks_of(ids.mix) == "s2,s1,s1", `exactly, got "{tracks_of(ids.mix)}"`)
for _, requested in { album, h1, nothing } do
	replaced, message = replace({ s1, requested })
	assert(replaced == 400 and message == "failed to update playlist", `not a visible track, got {replaced} {message}`)
end
assert(tracks_of(ids.mix) == "s2,s1,s1", "and the tracks are kept")
