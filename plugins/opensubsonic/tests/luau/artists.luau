local fixture, harness = ...
local call = require("./_call")(harness)
local covers = require("@lyra/covers")
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function cover_art_id(item_id: string): string
	local cover = covers.get({ item_id })[item_id]
	return `{cover.owner_id}.{string.sub(cover.hash, 1, 8)}`
end

-- Index names, then each index's artist ids, in order.
local function indexes(params: { [string]: string }?): ({ string }, { [string]: { string } })
	local artists = call.ok("getArtists", key, params).artists
	assert(artists.ignoredArticles == "", `ignoredArticles {artists.ignoredArticles}`)
	local names: { string } = {}
	local members: { [string]: { string } } = {}
	for _, index in artists.index or {} do
		table.insert(names, index.name)
		members[index.name] = call.ids(index.artist)
	end
	return names, members
end

-- Album artists the caller sees, under the first letter of their sort names or else their names,
-- accents and case aside; other first characters go under `#`, listed first. Hal, credited only
-- in a hidden library, and Zed, credited only on a song, are left out.
local names, members = indexes()
call.same(names, { "#", "A", "B", "E", "Z" }, "index names")
call.same(members["#"], { ids.pac }, "#")
call.same(members.A, { ids.amy }, "A")
call.same(members.B, { ids.bob }, "B")
call.same(members.E, { ids.emile }, "E")
call.same(members.Z, { ids.zebras }, "Z, by sort name")

local listed = call.ok("getArtists", key).artists.index
local amy = listed[2].artist[1]
assert(amy.name == "Amy" and amy.albumCount == 2, `Amy's albums {amy.albumCount}`)
assert(amy.starred == "2023-11-14T22:13:20Z", `Amy starred {amy.starred}`)
assert(amy.sortName == "", "sortName")
assert(amy.coverArt == cover_art_id(ids.amy), `Amy's coverArt {amy.coverArt}`)
local bob = listed[3].artist[1]
assert(bob.coverArt == nil and bob.starred == nil and bob.albumCount == 1, "Bob has no cover and no star")

local zebras = listed[5].artist[1]
assert(zebras.name == "The Zebras" and zebras.sortName == "Zebras, The", `sortName {zebras.sortName}`)

-- `musicFolderId` keeps the artists with an album in that library, counting only those albums.
local _, in_music = indexes({ musicFolderId = tostring(folders.id(fixture.libraries.music)) })
call.same(in_music.A, { ids.amy }, "Amy in Music")
local music_artists =
	call.ok("getArtists", key, { musicFolderId = tostring(folders.id(fixture.libraries.music)) }).artists
assert(music_artists.index[2].artist[1].albumCount == 1, "Amy's albums in Music")
assert(in_music.B == nil, "Bob isn't in Music")
call.same(in_music.E, { ids.emile }, "Émile in Music")
local extra_names, in_extra = indexes({ musicFolderId = tostring(folders.id(fixture.libraries.extra)) })
call.same(extra_names, { "A", "B" }, "index names in Extra")
call.same(in_extra.B, { ids.bob }, "Bob in Extra")
local extra_artists =
	call.ok("getArtists", key, { musicFolderId = tostring(folders.id(fixture.libraries.extra)) }).artists
assert(extra_artists.index[1].artist[1].albumCount == 1, "Amy's albums in Extra")
assert(call.error_code("getArtists", key, { musicFolderId = "1" }) == 70, "an unknown folder")
local hidden_folder = tostring(folders.id(fixture.libraries.hidden))
assert(call.error_code("getArtists", key, { musicFolderId = hidden_folder }) == 70, "a hidden folder")
assert(call.error_code("getArtists", key, { musicFolderId = "music" }) == 0, "a folder id is an integer")

local xml = call.raw("getArtists", { apiKey = key }).body
assert(string.find(xml, `<artists ignoredArticles="">`, 1, true), xml)
assert(string.find(xml, `<index name="A"><artist `, 1, true), xml)
local _, index_elements = string.gsub(xml, "<index ", "")
assert(index_elements == 5, `five index elements, got {index_elements}`)

-- An artist carries the albums crediting them that the caller sees, which `albumCount` counts.
local artist = call.ok("getArtist", key, { id = ids.amy }).artist
assert(artist.id == ids.amy and artist.name == "Amy" and artist.albumCount == 2, `albums {artist.albumCount}`)
assert(artist.coverArt == cover_art_id(ids.amy) and artist.starred ~= nil, "Amy's coverArt and star")
call.same(call.ids(artist.album), { ids.duo, ids.solo }, "Amy's albums")
assert(artist.album[1].coverArt == nil, "Duo has no cover")
assert(artist.album[2].coverArt == cover_art_id(ids.solo), `Solo's coverArt {artist.album[2].coverArt}`)
assert(artist.album[2].name == "Solo" and artist.album[2].artistId == ids.amy and artist.album[2].songCount == 1)

-- An artist credited only on songs is found, without albums.
local zed = call.ok("getArtist", key, { id = ids.zed }).artist
assert(zed.name == "Zed" and zed.albumCount == 0 and zed.album == nil and zed.coverArt == nil, "Zed")

local artist_xml = call.raw("getArtist", { apiKey = key, id = ids.amy }).body
assert(string.find(artist_xml, `<artist `, 1, true), artist_xml)
local _, album_elements = string.gsub(artist_xml, "<album ", "")
assert(album_elements == 2, `two album elements, got {album_elements}`)

assert(call.error_code("getArtist", key, {}) == 10, "getArtist needs an id")
assert(call.error_code("getArtist", key, { id = ids.hal }) == 70, "a hidden artist isn't found")
assert(call.error_code("getArtist", key, { id = ids.solo }) == 70, "an album isn't an artist")
assert(call.error_code("getArtist", key, { id = "not-an-id" }) == 70, "an unknown id isn't found")

-- `getArtistInfo2` and `getArtistInfo` take an artist, album or song id, and answer the artist's
-- description as its biography and its MusicBrainz id. Amy has neither, and Zed only an id of
-- another scheme, so their info is empty.
local bob_id = "0b0b0b0b-0000-4000-8000-000000000001"
for method, container in { getArtistInfo2 = "artistInfo2", getArtistInfo = "artistInfo" } do
	local function info_of(id: string, params: { [string]: string }?): any
		local query: { [string]: string } = { id = id }
		for name, value in params or {} do
			query[name] = value
		end
		return call.ok(method, key, query)[container]
	end
	for _, id in { ids.bob, ids.bobs, ids.b1 } do
		local info = info_of(id)
		assert(info.biography == "Bob's story." and info.musicBrainzId == bob_id, `{method} of {id}`)
	end
	local emile = info_of(ids.e1)
	assert(emile.biography == "Émile's story." and emile.musicBrainzId == nil, `{method}: Émile has only a biography`)
	local pac = info_of(ids.pacs)
	assert(
		pac.biography == nil and pac.musicBrainzId == "2bac2bac-0000-4000-8000-000000000002",
		`{method}: 2Pac has only an id`
	)
	local bob_xml = call.raw(method, { apiKey = key, id = ids.bob }).body
	assert(string.find(bob_xml, `<{container}><biography>Bob`, 1, true), bob_xml)
	assert(not string.find(bob_xml, "biography=", 1, true), bob_xml)
	assert(string.find(bob_xml, `<musicBrainzId>{bob_id}</musicBrainzId>`, 1, true), bob_xml)

	for _, id in { ids.amy, ids.solo, ids.s1, ids.zed } do
		local info = info_of(id, { count = "5", includeNotPresent = "true" })
		assert(type(info) == "table" and next(info) == nil, `{method} of {id}`)
	end
	local info_xml = call.raw(method, { apiKey = key, id = ids.amy }).body
	assert(string.find(info_xml, `<{container}/>`, 1, true), info_xml)

	assert(call.error_code(method, key, {}) == 10, `{method} needs an id`)
	assert(call.error_code(method, key, { id = ids.hal }) == 70, `{method}: a hidden artist has no info`)
	assert(call.error_code(method, key, { id = ids.secret }) == 70, `{method}: nor does a hidden album`)
	assert(call.error_code(method, key, { id = ids.h1 }) == 70, `{method}: nor a hidden song`)
	assert(call.error_code(method, key, { id = "not-an-id" }) == 70, `{method}: an unknown id`)
	assert(call.error_code(method, key, { id = ids.amy, count = "many" }) == 0, `{method}: a bad count`)
	assert(call.error_code(method, key, { id = ids.amy, includeNotPresent = "maybe" }) == 0, `{method}: a bad flag`)
end
