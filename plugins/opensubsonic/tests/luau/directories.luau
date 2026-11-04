local fixture, harness = ...
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")

local ids = fixture.ids
local key = fixture.api_keys.listener_key

local function folder_of(library: string): string
	return tostring(folders.id(fixture.libraries[library]))
end

-- Index names, then each index's artist ids, in order.
local function indexes(params: { [string]: string }?): ({ string }, { [string]: { string } })
	local result = call.ok("getIndexes", key, params).indexes
	assert(result.ignoredArticles == "", `ignoredArticles {result.ignoredArticles}`)
	assert(result.lastModified == 0, `lastModified {result.lastModified}`)
	assert(result.shortcut == nil and result.child == nil, "no shortcuts or root songs")
	local names: { string } = {}
	local members: { [string]: { string } } = {}
	for _, index in result.index or {} do
		table.insert(names, index.name)
		members[index.name] = call.ids(index.artist)
	end
	return names, members
end

-- The index lists the album artists `getArtists` does, as `Artist`s: Hal, credited only in a
-- hidden library, and Zed, credited only on a song, are left out.
local names, members = indexes()
call.same(names, { "A", "B" }, "index names")
call.same(members.A, { ids.amy }, "A")
call.same(members.B, { ids.bob }, "B")
local amy = call.ok("getIndexes", key).indexes.index[1].artist[1]
assert(amy.name == "Amy" and amy.starred == "2023-11-14T22:13:20Z" and amy.userRating == 4, "Amy")
assert(amy.albumCount == nil and amy.sortName == nil, "no ArtistID3 fields")
-- `ifModifiedSince` doesn't hide the index, as nothing tells when it last changed.
call.same(select(1, indexes({ ifModifiedSince = "4102444800000" })), { "A", "B" }, "if modified since")

local _, in_music = indexes({ musicFolderId = folder_of("music") })
call.same(in_music.A, { ids.amy }, "Amy in Music")
call.same(in_music.B, { ids.bob }, "Bob in Music")
assert(call.error_code("getIndexes", key, { musicFolderId = folder_of("hidden") }) == 70, "a hidden folder")
assert(call.error_code("getIndexes", key, { musicFolderId = "1" }) == 70, "an unknown folder")

local index_xml = call.raw("getIndexes", { apiKey = key }).body
assert(string.find(index_xml, '<indexes ignoredArticles="" lastModified="0">', 1, true), index_xml)

local function directory(id: string): any
	return call.ok("getMusicDirectory", key, { id = id }).directory
end

-- An artist's directory holds its albums, oldest first, then by name; it sits under no parent.
local amy_dir = directory(ids.amy)
assert(amy_dir.id == ids.amy and amy_dir.name == "Amy" and amy_dir.parent == nil, "Amy's directory")
assert(amy_dir.starred == "2023-11-14T22:13:20Z" and amy_dir.userRating == 4, "the caller's view of Amy")
call.same(call.ids(amy_dir.child), { ids.duo, ids.solo }, "Amy's albums")
for _, child in amy_dir.child do
	assert(child.isDir == true and child.parent == ids.amy, `{child.title} is a directory under Amy`)
end

-- An album sits under its first artist, in whichever artist's directory it is listed.
local bob_dir = directory(ids.bob)
call.same(call.ids(bob_dir.child), { ids.duo, ids.other }, "Bob's albums")
assert(bob_dir.child[1].parent == ids.amy, "Duo under Amy")

-- An artist without albums is an empty directory.
local zed_dir = directory(ids.zed)
assert(zed_dir.name == "Zed" and zed_dir.child == nil, "Zed's directory")

-- An album's directory holds its songs in disc and track order, under its first artist.
local solo_dir = directory(ids.solo)
assert(solo_dir.id == ids.solo and solo_dir.name == "Solo" and solo_dir.parent == ids.amy, "Solo's directory")
assert(solo_dir.starred == "2023-11-14T22:15:00Z" and solo_dir.userRating == 5, "the caller's view of Solo")
assert(solo_dir.playCount == 2, `Solo's plays {solo_dir.playCount}`)
call.same(call.ids(solo_dir.child), { ids.s1, ids.s2, ids.s3 }, "Solo's songs")
for _, child in solo_dir.child do
	assert(child.isDir == false and child.parent == ids.solo, `{child.title} is a song in Solo`)
end
local duo_dir = directory(ids.duo)
assert(duo_dir.parent == ids.amy, "Duo under Amy")
call.same(call.ids(duo_dir.child), { ids.d1 }, "Duo's songs")

-- A client following parents from a song reaches the album, then the artist listing it.
local song = call.ok("getSong", key, { id = ids.s3 }).song
local up = directory(song.parent)
assert(up.id == ids.solo, "a song's parent is its album")
local top = directory(up.parent)
assert(top.id == ids.amy and table.find(call.ids(top.child), ids.solo), "an album's parent lists it")
-- Albums listed elsewhere carry the same parent.
local listed = call.ok("getAlbumList", key, { type = "alphabeticalByName" }).albumList.album
for _, album in listed do
	assert(directory(album.parent).id == album.parent, `{album.title}'s parent is a directory`)
	assert(directory(album.id).id == album.id, `{album.title} is a directory`)
end

assert(call.error_code("getMusicDirectory", key, {}) == 10, "a directory needs an id")
assert(call.error_code("getMusicDirectory", key, { id = ids.secret }) == 70, "a hidden album")
assert(call.error_code("getMusicDirectory", key, { id = ids.hal }) == 70, "a hidden artist")
assert(call.error_code("getMusicDirectory", key, { id = ids.h1 }) == 70, "a hidden song")
assert(call.error_code("getMusicDirectory", key, { id = ids.s1 }) == 70, "a song isn't a directory")
assert(call.error_code("getMusicDirectory", key, { id = folder_of("music") }) == 70, "a music folder isn't one")
assert(call.error_code("getMusicDirectory", key, { id = "missing" }) == 70, "an unknown id")

local directory_xml = call.raw("getMusicDirectory", { apiKey = key, id = ids.solo }).body
local directory_tag = string.match(directory_xml, "<directory [^>]*>") or ""
assert(string.find(directory_tag, `id="{ids.solo}"`, 1, true), directory_xml)
assert(string.find(directory_tag, `parent="{ids.amy}"`, 1, true), directory_xml)
local _, child_elements = string.gsub(directory_xml, "<child ", "")
assert(child_elements == 3, `three child elements, got {child_elements}`)

-- What the caller starred and can see, as `Artist`s, directory albums and songs.
local starred = call.ok("getStarred", key).starred
call.same(call.ids(starred.artist), { ids.amy }, "starred artists")
call.same(call.ids(starred.album), { ids.solo }, "starred albums")
call.same(call.ids(starred.song), { ids.s2 }, "starred songs")
assert(starred.artist[1].userRating == 4 and starred.artist[1].albumCount == nil, "an Artist")
local starred_album = starred.album[1]
assert(starred_album.isDir == true and starred_album.parent == ids.amy and starred_album.title == "Solo", "a Child")
assert(starred_album.name == nil and starred_album.songCount == nil, "no AlbumID3 fields")
assert(starred.song[1].parent == ids.solo and starred.song[1].isDir == false, "a song")

local in_extra = call.ok("getStarred", key, { musicFolderId = folder_of("extra") }).starred
call.same(call.ids(in_extra.artist), { ids.amy }, "starred artists in Extra")
assert(in_extra.album == nil and in_extra.song == nil, "nothing else starred in Extra")
assert(call.error_code("getStarred", key, { musicFolderId = folder_of("hidden") }) == 70, "a hidden folder")
local starred_xml = call.raw("getStarred", { apiKey = key }).body
assert(string.find(starred_xml, "<starred><", 1, true), starred_xml)

-- `getStarred2` lists the same items.
local starred2 = call.ok("getStarred2", key).starred2
call.same(call.ids(starred2.artist), { ids.amy }, "starred2 artists")
call.same(call.ids(starred2.album), { ids.solo }, "starred2 albums")
call.same(call.ids(starred2.song), { ids.s2 }, "starred2 songs")
