local fixture = ...
local constants = require("../../constants")
local item_ids = require("../../protocol/items/identifiers")
local item_refresh = require("../../routes/item_refresh")
local libraries = require("@lyra/libraries")
local metadata = require("@lyra/metadata")
local task = require("@harmony/task")

-- Feishin's "Refresh metadata" posts to the item's Refresh route the way Jellyfin's web client
-- does, with both refresh modes set; upstream queues the refresh and answers 204 at once.

local ids = fixture.ids
local FULL = {
	metadataRefreshMode = { "FullRefresh" },
	imageRefreshMode = { "FullRefresh" },
	replaceAllMetadata = { "false" },
	replaceAllImages = { "false" },
}

local refreshed: { string } = {}
local provider = metadata.Provider.new("item-refresh-test")
provider:refresh(metadata.EntityType.Release, function(ctx: metadata.ReleaseRefreshContext)
	table.insert(refreshed, ctx.id :: string)
	return nil
end)

local function refresh(id: string, query: { [string]: { string } }?, key: string?): number
	local ctx = {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = `/Items/{id}/Refresh`,
			headers = {
				authorization = `MediaBrowser Client="Feishin", Device="Firefox", DeviceId="feishin", Version="0.12.0", Token="{key or fixture.api_keys.admin_key}"`,
			},
			query = query or FULL,
			body_raw = "",
		},
		params = { itemId = id },
	} :: any
	return item_refresh.handle_refresh_item(ctx).status
end

local function item(id: string): string
	return item_ids.encode_jellyfin_item_id(id) :: string
end

local function wait_for(done: () -> boolean, what: string)
	for _ = 1, 200 do
		if done() then
			return
		end
		task.wait(0.05)
	end
	error(`timed out waiting for {what}`)
end

-- An album refreshes in the background after the 204.
assert(refresh(item(ids.album)) == 204)
wait_for(function()
	return #refreshed == 1
end, "the album's refresh")
assert(refreshed[1] == ids.album)

-- Default asks the providers too; with no mode set, as with None, nothing is refreshed.
assert(refresh(item(ids.album), { metadataRefreshMode = { "default" } }) == 204)
wait_for(function()
	return #refreshed == 2
end, "the default refresh")
-- Only metadata modes ask the providers; Lyra syncs covers only after them.
assert(refresh(item(ids.album), { imageRefreshMode = { "FullRefresh" }, replaceAllImages = { "true" } }) == 204)
assert(refresh(item(ids.album), { metadataRefreshMode = { "FullRefresh" }, imageRefreshMode = { "None" } }) == 204)
wait_for(function()
	return #refreshed == 3
end, "the metadata-only refresh")
assert(refresh(item(ids.album), {}) == 204)
assert(refresh(item(ids.album), { metadataRefreshMode = { "ValidationOnly" }, imageRefreshMode = { "None" } }) == 204)

-- Artists, tracks and playlists answer 204 as well; only a release has a provider here.
assert(refresh(item(ids.singer)) == 204)
assert(refresh(item(ids.song)) == 204)
assert(refresh(item(ids.mix)) == 204)
-- Another user's private playlist and what the plugin shows as no item are not found.
assert(refresh(item(ids.private)) == 404)
assert(refresh(item(fixture.users.listener)) == 404)

-- A library syncs whatever the modes, as upstream always scans a library's folders.
local music = fixture.libraries.music
for _, query in { {}, { metadataRefreshMode = { "ValidationOnly" }, imageRefreshMode = { "None" } }, FULL } do
	local before = libraries.sync_status({ music })[music].id
	assert(refresh(item(music), query) == 204)
	local run = libraries.sync_status({ music })[music]
	assert(run.kind == "library_sync" and run.id ~= nil and run.id ~= before, "a new sync started")
	wait_for(function()
		return libraries.sync_status({ music })[music].finished_at_ms ~= nil
	end, "the library's sync")
end
assert(#refreshed == 3, "no library refresh ran")

-- The root folder syncs every library; the playlists folder refreshes nothing.
constants.configure_root_item_id(string.rep("1", 32))
local talks = fixture.libraries.talks
local scanned = libraries.sync_status({ music, talks })
assert(refresh(constants.ROOT_ITEM_ID) == 204)
local rescanned = libraries.sync_status({ music, talks })
for _, library_id in { music, talks } do
	assert(rescanned[library_id].kind == "library_sync", "the library syncs")
	assert(rescanned[library_id].id ~= scanned[library_id].id, "a new sync started")
end
wait_for(function()
	local runs = libraries.sync_status({ music, talks })
	return runs[music].finished_at_ms ~= nil and runs[talks].finished_at_ms ~= nil
end, "the root's syncs")
assert(refresh(constants.PLAYLISTS_FOLDER_ID) == 204)
assert(libraries.sync_status({ music })[music].id == rescanned[music].id, "the playlists folder syncs nothing")
assert(#refreshed == 3, "nothing reached the provider")

-- A metadata FullRefresh asks the providers about every release, other modes only about the
-- changed ones. The image mode picks the covers; a FullRefresh looks them up again and, with
-- ReplaceAllImages, which applies to it alone, replaces them.
local function sync_options(metadata_mode: string, image_mode: string, replace: boolean): string
	local options = item_refresh.library_sync_options(metadata_mode :: any, image_mode :: any, replace)
	return `{options.releases} {options.covers}{if options.bypass_cover_cache then "+bypass" else ""}`
end
assert(sync_options("None", "None", true) == "changed skip")
assert(sync_options("ValidationOnly", "ValidationOnly", false) == "changed skip")
assert(sync_options("Default", "Default", false) == "changed missing")
assert(sync_options("Default", "Default", true) == "changed missing")
assert(sync_options("FullRefresh", "None", false) == "all skip")
assert(sync_options("FullRefresh", "FullRefresh", false) == "all missing+bypass")
assert(sync_options("FullRefresh", "FullRefresh", true) == "all replace+bypass")
assert(sync_options("None", "FullRefresh", true) == "changed replace+bypass")

-- Unknown items, malformed ids and malformed parameters. Upstream binds the id as a Guid.
assert(refresh(string.rep("a", 32)) == 404)
assert(refresh(string.rep("0", 32)) == 400, "the empty Guid")
assert(refresh("00000000-0000-0000-0000-000000000000") == 400)
assert(refresh("not-an-item") == 400)
assert(refresh(item(ids.album) .. "0") == 400)
assert(refresh(item(ids.album), { metadataRefreshMode = { "Everything" } }) == 400)
assert(refresh(item(ids.album), { metadataRefreshMode = { "FullRefresh" }, replaceAllImages = { "yes" } }) == 400)

-- Like upstream, refreshing is an administrator's.
assert(refresh(item(ids.album), nil, "not-a-key") == 401)
assert(refresh(item(ids.album), nil, fixture.api_keys.listener_key) == 403)
