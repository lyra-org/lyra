local fixture, harness = ...
local artists = require("@lyra/artists")
local auth = require("@lyra/auth")
local releases = require("@lyra/releases")
local tracks = require("@lyra/tracks")

local albums = require("../../catalog/albums")
local artist_items = require("../../catalog/artists")
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")
local response = require("../../protocol/response")
local songs = require("../../catalog/songs")

-- `getStarred` and `getStarred2` stream the document `ok` answers with the starred lists whole,
-- byte for byte, in every format.

local key = fixture.api_keys.listener_key

local function query(params: { [string]: string }): { [string]: { string } }
	local result = {}
	for name, value in params do
		result[name] = { value }
	end
	return result
end

-- What the method answered before it streamed: each starred list queried whole.
local function whole(params: { [string]: string }, container: string, artist_list: any, album_list: any): any
	return harness.plugin:call(function()
		assert(auth.resolve_auth(key), "authenticated")
		local library_id = folders.library_id(query(params))
		local starred = {
			favorite = true,
			library_id = library_id,
			available = true,
			sort = { { key = "favorited_at", order = "descending" } },
		}
		local result: { [string]: any } = {}
		local found_artists = artist_list(artists.query(starred).items)
		local found_albums = album_list(releases.query(starred).items)
		local found_songs = songs.list(tracks.query(starred).items)
		if #found_artists > 0 then
			result.artist = found_artists
		end
		if #found_albums > 0 then
			result.album = found_albums
		end
		if #found_songs > 0 then
			result.song = found_songs
		end
		return response.ok(query(params), { [container] = if next(result) then result else response.EMPTY_OBJECT })
	end)
end

local extra = tostring(folders.id(fixture.libraries.extra))
local listed = 0
for _, method in
	{
		{ name = "getStarred", container = "starred", artists = artist_items.directories, albums = albums.children },
		{ name = "getStarred2", container = "starred2", artists = artist_items.list, albums = albums.list },
	}
do
	for _, format in { { f = "json" }, { f = "jsonp", callback = "handle" }, {} } :: { { [string]: string } } do
		for _, folder in { {}, { musicFolderId = extra } } :: { { [string]: string } } do
			local params: { [string]: string } = table.clone(format)
			for name, value in folder do
				params[name] = value
			end
			params.apiKey = key
			local streamed = call.raw(method.name, params)
			assert(streamed.kind == "stream", `{method.name} answered {streamed.kind}`)
			params.v, params.c = "1.16.1", "test"
			local expected = whole(params, method.container, method.artists, method.albums)
			assert(streamed.headers["content-type"] == expected.headers["content-type"], method.name)
			local body = call.body(streamed)
			assert(body == expected.body, `{method.name} {format.f or "xml"}\nstreamed\n{body}\nwhole\n{expected.body}`)
			listed += 1
		end
	end
end
assert(listed == 12, `{listed} documents compared`)
