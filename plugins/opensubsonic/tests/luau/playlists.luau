local fixture = ...
local call = require("./_call")

local ids = fixture.ids
local key = fixture.api_keys.owner_key
local other_key = fixture.api_keys.other_key

local function playlist(id: string): any
	return call.ok("getPlaylist", key, { id = id }).playlist
end

local function entries(id: string): { string }
	return call.ids(playlist(id).entry)
end

-- A user lists its own playlists and everyone's public ones.
local listed = call.ok("getPlaylists", key).playlists.playlist
local by_id = {}
for _, item in listed do
	by_id[item.id] = item
end
assert(#listed == 2 and by_id[ids.mine] and by_id[ids.shared], `own and public, got {#listed}`)
local mine = by_id[ids.mine]
assert(mine.name == "Mine" and mine.owner == "owner" and mine.public == false and mine.readonly == false)
assert(mine.songCount == 4 and mine.duration == 270, `mine totals {mine.songCount} {mine.duration}`)
assert(mine.created ~= nil and mine.changed ~= nil)
local shared = by_id[ids.shared]
assert(shared.owner == "other" and shared.public == true and shared.readonly == true)
assert(shared.songCount == 1, "a hidden song isn't counted")

-- Only an admin names a user, even the caller itself.
assert(call.error_code("getPlaylists", key, { username = "other" }) == 50, "a listener can't list another user's")
assert(call.error_code("getPlaylists", key, { username = "owner" }) == 50, "nor name itself")
local admin_own = call.ok("getPlaylists", fixture.api_keys.admin_key, { username = "ADMIN" }).playlists.playlist
call.same(call.ids(admin_own), { ids.shared }, "an admin naming itself lists as without a name")
local admin_listed = call.ok("getPlaylists", fixture.api_keys.admin_key, { username = "other" }).playlists.playlist
call.same(call.ids(admin_listed), { ids.shared }, "the other user's public ones, even as an admin")
assert(
	call.error_code("getPlaylist", fixture.api_keys.admin_key, { id = ids.theirs }) == 70,
	"an admin doesn't see another user's private playlist"
)
assert(call.error_code("getPlaylists", fixture.api_keys.admin_key, { username = "nobody" }) == 70, "an unknown user")

-- A playlist carries its visible entries, in order and with repeats.
call.same(entries(ids.mine), { ids.a, ids.b, ids.c, ids.a }, "entries")
assert(playlist(ids.mine).entry[2].title == "B")
call.same(entries(ids.shared), { ids.a }, "another user's public playlist")
assert(call.error_code("getPlaylist", key, { id = ids.theirs }) == 70, "another user's private playlist")
assert(call.error_code("getPlaylist", key, {}) == 10, "getPlaylist needs an id")
assert(call.error_code("getPlaylist", key, { id = "not-an-id" }) == 70, "a malformed id isn't found")
assert(call.error_code("deletePlaylist", key, { id = "not-an-id" }) == 70, "nor deleted")

-- Creating takes repeated songIds and answers with the playlist.
local created = call.ok("createPlaylist", key, { name = "New", songId = { ids.c, ids.a, ids.c } }).playlist
assert(created.name == "New" and created.owner == "owner" and created.songCount == 3)
call.same(call.ids(created.entry), { ids.c, ids.a, ids.c }, "created entries")
local empty = call.ok("createPlaylist", key, { name = "Empty" }).playlist
assert(empty.songCount == 0 and empty.entry == nil, "an empty playlist")
assert(call.error_code("createPlaylist", key, {}) == 10, "a new playlist needs a name")
assert(call.error_code("createPlaylist", key, { name = "Bad", songId = ids.h }) == 70, "a hidden song")
assert(call.error_code("createPlaylist", key, { name = "Bad", songId = "nothing" }) == 70, "an unknown song")

-- With a playlistId, its songs are replaced.
local replaced = call.ok("createPlaylist", key, { playlistId = created.id, songId = ids.b }).playlist
assert(replaced.id == created.id and replaced.name == "New")
call.same(call.ids(replaced.entry), { ids.b }, "replaced entries")
assert(call.error_code("createPlaylist", key, { playlistId = ids.shared, songId = ids.b }) == 50, "not the owner's")

-- Updating removes by index as the caller sees the playlist, then appends.
call.ok("updatePlaylist", key, {
	playlistId = ids.mine,
	songIndexToRemove = { "3", "0" },
	songIdToAdd = { ids.c },
	name = "Renamed",
	comment = "Notes",
	public = "true",
})
local updated = playlist(ids.mine)
assert(updated.name == "Renamed" and updated.comment == "Notes" and updated.public == true)
call.same(call.ids(updated.entry), { ids.b, ids.c, ids.c }, "updated entries")
call.ok("updatePlaylist", key, { playlistId = ids.mine, songIndexToRemove = { "2", "1", "0" } })
assert(playlist(ids.mine).entry == nil, "every entry removed, highest index first")
call.ok("updatePlaylist", key, { playlistId = ids.mine, public = "false" })
assert(playlist(ids.mine).public == false and playlist(ids.mine).name == "Renamed", "fields left alone")

assert(call.error_code("updatePlaylist", key, {}) == 10, "updating needs a playlistId")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.theirs }) == 70, "a private playlist of another")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.shared, name = "Mine now" }) == 50, "a public one")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, songIndexToRemove = "5" }) == 70, "out of range")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, songIndexToRemove = "x" }) == 0, "not an index")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, public = "maybe" }) == 0, "not a boolean")
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, songIdToAdd = ids.h }) == 70, "a hidden song")
call.same(call.ids(call.ok("getPlaylist", other_key, { id = ids.shared }).playlist.entry), { ids.a }, "untouched")

-- A change past the playlist cap is refused before anything is written.
local playlists_module = require("@lyra/playlists")
local before = playlist(ids.mine)
local too_many = table.create(playlists_module.MAX_TRACK_IDS + 1, ids.a)
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, name = "Capped", songIdToAdd = too_many }) == 0)
assert(call.error_code("createPlaylist", key, { name = "Capped", songId = too_many }) == 0)
local after = playlist(ids.mine)
assert(after.name == before.name and after.songCount == before.songCount, "nothing was written")

-- A name the host refuses fails before any entry changes.
call.ok("updatePlaylist", key, { playlistId = ids.mine, songIdToAdd = { ids.a } })
local named = playlist(ids.mine)
assert(call.error_code("updatePlaylist", key, { playlistId = ids.mine, name = "   ", songIndexToRemove = "0" }) == 0)
assert(playlist(ids.mine).songCount == named.songCount, "a blank name leaves the entries alone")

-- Form-encoded POST bodies carry parameters, including the credentials.
local form = call.raw("createPlaylist", {}, {
	http_method = "POST",
	headers = { ["content-type"] = "application/x-www-form-urlencoded; charset=utf-8" },
	body = `apiKey={key}&f=json&name=Form+list%21&songId={ids.a}&songId={ids.b}`,
})
local serde = require("@harmony/serde")
local form_body = (serde.json.decode(form.body) :: any)["subsonic-response"]
assert(form_body.status == "ok" and form_body.playlist.name == "Form list!", `form post {form.body}`)
call.same(call.ids(form_body.playlist.entry), { ids.a, ids.b }, "form post songs")

-- Only the owner deletes, and a deleted playlist is gone.
assert(call.error_code("deletePlaylist", key, { id = ids.shared }) == 50, "another user's public playlist")
assert(call.error_code("deletePlaylist", key, { id = ids.theirs }) == 70, "another user's private playlist")
assert(call.error_code("deletePlaylist", key, {}) == 10, "deleting needs an id")
call.ok("deletePlaylist", key, { id = created.id })
assert(call.error_code("getPlaylist", key, { id = created.id }) == 70, "deleted")
