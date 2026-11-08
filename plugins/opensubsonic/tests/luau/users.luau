local fixture, harness = ...
local call = require("./_call")(harness)
local folders = require("../../catalog/folders")

local libraries = fixture.libraries
local listener_key = fixture.api_keys.listener_key
local downloader_key = fixture.api_keys.downloader_key
local admin_key = fixture.api_keys.admin_key
local lonely_key = fixture.api_keys.lonely_key
local serde = require("@harmony/serde")

local function folder_ids(keys: { string }): { number }
	local ids = {}
	for _, key in keys do
		table.insert(ids, folders.id(libraries[key]))
	end
	table.sort(ids)
	return ids
end

-- The folder ids, in no particular order.
local function same_folders(user: any, keys: { string }, label: string)
	local ids = table.clone(user.folder)
	table.sort(ids)
	call.same(ids, folder_ids(keys), label)
end

-- The license is always valid, with no email or expiry.
local license = call.ok("getLicense", listener_key).license
assert(license.valid == true, "valid")
assert(license.email == nil and license.licenseExpires == nil and license.trialExpires == nil, "nothing else")

-- A listener's own user, named in any case, with the libraries it sees as folders.
local listener = call.ok("getUser", listener_key, { username = "LISTENER" }).user
assert(listener.username == "listener", listener.username)
same_folders(listener, { "music" }, "the listener's folders")
local roles = {
	scrobblingEnabled = true,
	adminRole = false,
	settingsRole = false,
	downloadRole = false,
	uploadRole = false,
	playlistRole = true,
	coverArtRole = true,
	commentRole = false,
	podcastRole = false,
	streamRole = true,
	jukeboxRole = false,
	shareRole = false,
	videoConversionRole = false,
}
for role, expected in roles do
	assert(listener[role] == expected, `listener {role}: {listener[role]}`)
end
assert(listener.email == nil and listener.maxBitRate == nil, "no email or bitrate limit")

-- The download permission gives the download role.
local downloader = call.ok("getUser", downloader_key, { username = "downloader" }).user
assert(downloader.downloadRole == true and downloader.adminRole == false, "the downloader's roles")
same_folders(downloader, { "music", "extra" }, "the downloader's folders")

-- An admin may download and sees every library.
local admin = call.ok("getUser", admin_key, { username = "admin" }).user
assert(admin.adminRole == true and admin.downloadRole == true, "the admin's roles")
same_folders(admin, { "music", "extra", "locked" }, "the admin's folders")

-- Only an admin may name another user, and sees that user's folders.
assert(call.error_code("getUser", listener_key, { username = "admin" }) == 50, "a listener naming the admin")
assert(call.error_code("getUser", listener_key) == 10, "the username is required")
local named = call.ok("getUser", admin_key, { username = "Downloader" }).user
assert(named.username == "downloader" and named.downloadRole == true, "the admin reads the downloader")
same_folders(named, { "music", "extra" }, "the downloader's folders, read by the admin")
same_folders(call.ok("getUser", admin_key, { username = "listener" }).user, { "music" }, "the listener's folders")
local deputy = call.ok("getUser", admin_key, { username = "deputy" }).user
assert(deputy.adminRole == true, "the other admin")
same_folders(deputy, { "music", "extra", "locked" }, "another admin's folders")
assert(call.error_code("getUser", admin_key, { username = "nobody" }) == 70, "an unknown user")

-- Only an admin lists the users, each as getUser gives it.
assert(call.error_code("getUsers", listener_key) == 50, "a listener can't list users")
assert(call.error_code("getUsers", downloader_key) == 50, "nor can a user with other permissions")
local listed: { [string]: any } = {}
for _, user in call.ok("getUsers", admin_key).users.user do
	listed[user.username] = user
end
for _, name in { "listener", "downloader", "admin", "deputy" } do
	assert(listed[name], `{name} is listed`)
end
same_folders(listed.listener, { "music" }, "the listed listener's folders")
same_folders(listed.downloader, { "music", "extra" }, "the listed downloader's folders")
same_folders(listed.deputy, { "music", "extra", "locked" }, "the listed admin's folders")
assert(listed.listener.adminRole == false and listed.listener.downloadRole == false, "the listed listener's roles")
assert(listed.downloader.downloadRole == true and listed.deputy.adminRole == true, "the listed roles")

-- While authentication is on the default user is no account, though what it left behind stays.
assert(listed.default == nil, "the default user isn't listed")
assert(call.error_code("getUser", admin_key, { username = "default" }) == 70, "nor found by name")
assert(call.error_code("getPlaylists", admin_key, { username = "default" }) == 70, "nor named for its playlists")
local left_behind: any = nil
for _, item in call.ok("getPlaylists", listener_key).playlists.playlist do
	if item.id == fixture.ids.left_behind then
		left_behind = item
	end
end
assert(left_behind and left_behind.owner == nil and left_behind.readonly == true, "its public playlist has no owner")

-- XML carries roles as attributes and folders as elements.
local xml = call.raw("getUser", { apiKey = downloader_key, username = "downloader" }).body
assert(string.find(xml, `downloadRole="true"`, 1, true), xml)
for _, id in folder_ids({ "music", "extra" }) do
	assert(string.find(xml, `<folder>{id}</folder>`, 1, true), xml)
end

-- A user seeing no library has an empty folder list, not a missing one or an object.
local lonely = call.raw("getUser", { apiKey = lonely_key, username = "lonely", f = "json" }).body
assert(string.find(lonely, '"folder":%s*%[%]'), lonely)

-- A client signing in with the username and the API key as password, as Feishin does, gets every
-- classic role as a boolean and the folders as integers.
local signed_in =
	call.raw("getUser", { u = "downloader", p = downloader_key, username = "downloader", f = "json" }).body
local user = (serde.json.decode(signed_in) :: any)["subsonic-response"].user
assert(type(user.username) == "string" and user.username == "downloader", signed_in)
for _, role in
	{
		"scrobblingEnabled",
		"adminRole",
		"settingsRole",
		"downloadRole",
		"uploadRole",
		"playlistRole",
		"coverArtRole",
		"commentRole",
		"podcastRole",
		"streamRole",
		"jukeboxRole",
		"shareRole",
		"videoConversionRole",
	}
do
	assert(type(user[role]) == "boolean", `{role} is a boolean: {signed_in}`)
end
assert(type(user.folder) == "table" and #user.folder == 2, signed_in)
for _, id in user.folder do
	assert(type(id) == "number" and id == math.floor(id), `folder ids are integers: {signed_in}`)
end
