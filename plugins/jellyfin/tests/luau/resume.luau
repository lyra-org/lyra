local fixture, harness = ...
local bookmarks = require("@lyra/bookmarks")
local serde = require("@harmony/serde")

local item_ids = require("../../protocol/items/identifiers")
local listing = require("../../protocol/items/listing")
local options = require("../../protocol/items/options")
local preferences = require("../../protocol/preferences")
local resume = require("../../routes/resume")

-- Upstream's resume lists hold every item with a playback position: for Lyra, the tracks the caller
-- bookmarked and can see, the most recently saved first.

local ids = fixture.ids
local AUTHORIZATION = `MediaBrowser Token="{fixture.api_keys.phone}"`

local function hex(id: string): string
	return item_ids.encode_jellyfin_item_id(id) :: string
end

local function call(params: { [string]: string }?, user_id: string?): any
	local query: { [string]: { string } } = {}
	for key, value in params or {} do
		query[key] = { value }
	end
	return harness.plugin:call(
		resume.handle_resume,
		{
			plugin_id = "jellyfin",
			request = {
				method = "GET",
				path = if user_id then `/Users/{user_id}/Items/Resume` else "/UserItems/Resume",
				headers = { authorization = AUTHORIZATION },
				query = query,
				body_raw = "",
				json = nil,
			},
			params = if user_id then { userId = user_id } else {},
		} :: any
	)
end

local function list(params: { [string]: string }?, user_id: string?): any
	local response = call(params, user_id)
	assert(response.status == 200, `listing: {response.status}`)
	if response.kind == "stream" then
		return serde.json.decode(harness.plugin:read_body(response))
	end
	return response.body
end

local function names(result: any): string
	local found: { string } = {}
	for _, item in result.Items do
		table.insert(found, item.Name)
	end
	return table.concat(found, ",")
end

local function expect(result: any, expected: string, total: number?, label: string)
	local got = names(result)
	assert(got == expected, `{label}: listed {got}, expected {expected}`)
	local expected_total = total or #result.Items
	assert(
		result.TotalRecordCount == expected_total,
		`{label}: total {result.TotalRecordCount}, expected {expected_total}`
	)
end

-- Every bookmarked track the caller can see, newest bookmark first; the unbookmarked one and the
-- hidden one don't list.
local all = list()
expect(all, "Talk,Last,Middle,First", nil, "all")
assert(all.StartIndex == 0, `start {all.StartIndex}`)

-- The items are full listing items, with their user data.
local item = all.Items[2]
assert(item.Id == hex(ids.last) and item.Type == "Audio", "a track item")
assert(item.UserData.PlaybackPositionTicks == 30000 * 10000, `position {item.UserData.PlaybackPositionTicks}`)
assert(item.AlbumId == hex(ids.album), "with its album")
local found = listing.find_item_by_id(ids.last, fixture.users.listener) :: any
assert(found.Name == item.Name and found.RunTimeTicks == item.RunTimeTicks, "as other listings show it")

-- The legacy route answers alike for the caller. Another user's list is forbidden, as upstream
-- answers it, whether the path or the query names them.
expect(list(nil, hex(fixture.users.listener)), "Talk,Last,Middle,First", nil, "legacy")
local function expect_forbidden(response: any, label: string)
	assert(response.status == 403, `{label}: {response.status}`)
	assert(response.kind == "text" and response.body == "Error processing request.", `{label}: body {response.body}`)
end
expect_forbidden(call(nil, string.rep("b", 32)), "another user")
expect_forbidden(call({ userId = string.rep("b", 32) }), "another user by query")

-- Paging keeps the total.
local page = list({ StartIndex = "1", Limit = "2" })
expect(page, "Last,Middle", 4, "page")
assert(page.StartIndex == 1, `page start {page.StartIndex}`)
expect(list({ StartIndex = "4" }), "", 4, "past the end")
expect(list({ Limit = "0" }), "", 4, "no items")
-- Without the total, as upstream, the page's own count stands in for it.
expect(list({ Limit = "1", EnableTotalRecordCount = "false" }), "Talk", 1, "uncounted page")
expect(list({ StartIndex = "3", EnableTotalRecordCount = "false" }), "First", 1, "uncounted last page")
expect(list({ Limit = "1", EnableTotalRecordCount = "true" }), "Talk", 4, "counted page")

-- Only types that may be audio list.
expect(list({ MediaTypes = "Video" }), "", 0, "video")
expect(list({ IncludeItemTypes = "Movie,Episode" }), "", 0, "movies and episodes")
expect(list({ IncludeItemTypes = "MusicAlbum" }), "", 0, "albums")
expect(list({ ExcludeItemTypes = "Audio" }), "", 0, "audio excluded")
expect(list({ MediaTypes = "Audio" }), "Talk,Last,Middle,First", nil, "audio")
expect(list({ MediaTypes = "Video,Audio", IncludeItemTypes = "Audio,Movie" }), "Talk,Last,Middle,First", nil, "mixed")

-- A parent scopes the list to its tracks, as other listings scope them.
expect(list({ ParentId = hex(ids.album) }), "Last,First", nil, "album")
expect(list({ ParentId = hex(ids.amy) }), "Middle", nil, "artist")
expect(list({ ParentId = hex(fixture.libraries.music) }), "Last,Middle,First", nil, "library")
expect(list({ ParentId = hex(fixture.libraries.closed) }), "", 0, "a library the caller can't see")
expect(list({ ParentId = hex(ids.plain) }), "", 0, "a track")

-- A search finds among them.
expect(list({ SearchTerm = "Last" }), "Last", nil, "search")

-- Parameters the resume routes don't take are ignored.
expect(
	list({ SortBy = "SortName", Filters = "IsFavorite", IsPlayed = "true" }),
	"Talk,Last,Middle,First",
	nil,
	"ignored"
)

-- What the caller is playing now leaves the list when asked.
expect(list({ ExcludeActiveSessions = "true" }), "Talk,Middle,First", nil, "playing excluded")
expect(list({ ExcludeActiveSessions = "false" }), "Talk,Last,Middle,First", nil, "playing kept")
assert(call({ ExcludeActiveSessions = "maybe" }).status == 400, "an invalid flag")

-- `Filters=IsResumable` on other listings keeps the bookmarked tracks too, in their own order.
local resumable_options = options.for_user(
	options.parse({ Recursive = { "true" }, Filters = { "IsResumable" } } :: any),
	fixture.users.listener
)
local resumable_items, resumable_total = listing.list_items(resumable_options)
local resumable = { Items = resumable_items, TotalRecordCount = resumable_total }
expect(resumable, "Middle,Talk,First,Last", nil, "IsResumable filter")

-- As upstream, without a parent the libraries the caller excludes from its latest media leave the
-- list; a parent lists its tracks whatever the excludes.
local function exclude_from_latest(libraries: { string })
	local body = { LatestItemsExcludes = libraries }
	local ctx = { request = { json = body, body_raw = "{}" } }
	assert(preferences.user_configuration_set(ctx :: any, fixture.users.listener).status == 204, "excludes saved")
end
local music, spoken = hex(fixture.libraries.music), hex(fixture.libraries.spoken)
exclude_from_latest({ spoken })
expect(list(), "Last,Middle,First", nil, "spoken excluded")
expect(list(nil, hex(fixture.users.listener)), "Last,Middle,First", nil, "spoken excluded, legacy")
expect(list({ ParentId = spoken }), "Talk", nil, "the excluded library as the parent")
exclude_from_latest({ music })
expect(list(), "Talk", nil, "music excluded")
exclude_from_latest({ music, spoken })
expect(list(), "", 0, "every library excluded")
exclude_from_latest({})
expect(list(), "Talk,Last,Middle,First", nil, "none excluded")

-- Saving a position moves the track to the top; clearing it removes the track.
assert(bookmarks.set(ids.first, 15000), "moved First")
expect(list(), "First,Talk,Last,Middle", nil, "moved")
assert(bookmarks.remove(ids.talk), "cleared Talk")
expect(list(), "First,Last,Middle", nil, "cleared")
