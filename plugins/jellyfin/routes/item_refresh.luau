--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local libraries = require("@lyra/libraries")
local metadata = require("@lyra/metadata")

local task = require("@harmony/task")

local constants = require("../constants")
local item_ids = require("../protocol/items/identifiers")
local lookups = require("../protocol/items/lookups")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local scheduled_tasks = require("./scheduled_tasks")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type CoverUpdate = libraries.CoverUpdate

-- Upstream's MetadataRefreshMode, by name or number.
type RefreshMode = "None" | "ValidationOnly" | "Default" | "FullRefresh"

local REFRESH_MODES: { [string]: RefreshMode } = {
	none = "None",
	["0"] = "None",
	validationonly = "ValidationOnly",
	["1"] = "ValidationOnly",
	default = "Default",
	["2"] = "Default",
	fullrefresh = "FullRefresh",
	["3"] = "FullRefresh",
}

-- The kinds of entity the plugin shows as items; other ids name no item.
local ITEM_KINDS: { [string]: true } = {
	Library = true,
	Release = true,
	Artist = true,
	Track = true,
	Playlist = true,
	Genre = true,
}

local module = {}

local function refresh_mode(ctx: ApiContext, key: string): (RefreshMode?, ApiResponse?)
	local raw = requests.parse_query_string(ctx.request.query, { key })
	if raw == nil then
		return "None", nil
	end
	local mode = REFRESH_MODES[string.lower(raw)]
	if not mode then
		return nil, responses.bad_request(`The value '{raw}' is not valid for {key}.`)
	end
	return mode, nil
end

local function flag(ctx: ApiContext, key: string): (boolean?, ApiResponse?)
	local value, invalid = requests.parse_query_bool(ctx.request.query, key)
	if invalid then
		return nil, responses.bad_request(`The value '{invalid}' is not valid for {key}.`)
	end
	return value or false, nil
end

-- Whether a mode asks the providers, as upstream's Default and FullRefresh do; None and
-- ValidationOnly only look at what is stored.
local function asks_providers(mode: RefreshMode): boolean
	if mode == "Default" or mode == "FullRefresh" then
		return true
	elseif mode == "None" or mode == "ValidationOnly" then
		return false
	else
		local _: never = mode
		return false
	end
end

-- The covers an image mode fetches, and whether it looks them up again: none for None and
-- ValidationOnly, which fetch no remote images, else the missing ones. A FullRefresh looks them up
-- again and, with ReplaceAllImages, replaces them; as upstream's, ReplaceAllImages applies to a
-- FullRefresh alone.
local function cover_options(mode: RefreshMode, replace_images: boolean): (CoverUpdate, boolean)
	if not asks_providers(mode) then
		return "skip", false
	end
	local full = mode == "FullRefresh"
	return if full and replace_images then "replace" else "missing", full
end

-- A library's sync asks the providers about every release for a metadata FullRefresh, and
-- otherwise about the releases its scan found new or changed, with the image mode's covers.
function module.library_sync_options(
	metadata_mode: RefreshMode,
	image_mode: RefreshMode,
	replace_images: boolean
): libraries.SyncOptions
	local covers, bypass_cover_cache = cover_options(image_mode, replace_images)
	return {
		releases = if metadata_mode == "FullRefresh" then "all" else "changed",
		covers = covers,
		bypass_cover_cache = bypass_cover_cache,
	}
end

local function run_in_background(label: string, refresh: () -> ...any)
	task.spawn(function()
		local ok, err = pcall(refresh)
		if not ok then
			warn(`{label} failed: {err}`)
		end
	end)
end

-- Upstream queues the refresh and answers at once. Releases, artists and tracks refresh from the
-- providers. A library syncs whatever the modes, since upstream always validates a collection
-- folder's children, scanning its files, and the root folder syncs every library the caller can
-- manage. Lyra keeps no metadata of its own for other items, such as the playlists folder, so they
-- answer as upstream does without refreshing anything.
--
-- The modes map onto Lyra's refresh and sync as far as they allow, with the image mode picking the
-- covers as `cover_options` does. For a release, artist or track, a metadata mode that asks the
-- providers refreshes the metadata and those covers. Diverging from upstream, an image mode alone
-- refreshes nothing, since Lyra syncs covers only after the providers refreshed the metadata. A
-- library's sync takes `library_sync_options`; for None and ValidationOnly it still asks the
-- providers about the releases its scan found new or changed, as every Lyra sync does. With image
-- None those releases fetch no provider covers, as upstream's children fetch no remote images
-- then; their local covers are still read. Lyra's providers decide what metadata to replace, so
-- ReplaceAllMetadata and RegenerateTrickplay change nothing.
function module.handle_refresh_item(ctx: ApiContext): ApiResponse
	local denied = session.require_admin(ctx)
	if denied then
		return denied
	end
	local metadata_mode, metadata_err = refresh_mode(ctx, "metadataRefreshMode")
	if not metadata_mode then
		return metadata_err or responses.bad_request()
	end
	local image_mode, image_err = refresh_mode(ctx, "imageRefreshMode")
	if not image_mode then
		return image_err or responses.bad_request()
	end
	local replace_images, replace_err = flag(ctx, "replaceAllImages")
	if replace_images == nil then
		return replace_err or responses.bad_request()
	end
	for _, key in { "replaceAllMetadata", "regenerateTrickplay" } do
		local value, err = flag(ctx, key)
		if value == nil then
			return err or responses.bad_request()
		end
	end

	-- Upstream binds the id as a Guid, so one that isn't one is a bad request, as is the empty one,
	-- which it refuses to look up.
	local raw_id = ctx.params.itemId
	local normalized = constants.normalize_item_id(raw_id)
	if normalized == constants.EMPTY_ITEM_ID then
		return responses.bad_request("Guid can't be empty")
	end
	if constants.is_root_item_id(raw_id) then
		scheduled_tasks.start_scan(module.library_sync_options(metadata_mode, image_mode, replace_images))
		return api.response.empty(204)
	end
	if constants.is_playlists_folder_id(raw_id) then
		return api.response.empty(204)
	end
	local item_id = item_ids.decode_jellyfin_item_id(raw_id)
	if not item_id then
		return responses.bad_request(`The value '{raw_id or ""}' is not valid.`)
	end
	local principal = session.require_jellyfin_principal(ctx)
	local item_type = lookups.visible_kind(item_id, if principal then principal.user_id else nil)
	if not item_type or not ITEM_KINDS[item_type] then
		return responses.not_found("item not found")
	end

	if item_type == "Library" then
		libraries.sync(item_id, module.library_sync_options(metadata_mode, image_mode, replace_images))
	elseif
		(item_type == "Release" or item_type == "Artist" or item_type == "Track") and asks_providers(metadata_mode)
	then
		local covers, bypass_cover_cache = cover_options(image_mode, replace_images)
		run_in_background(`refresh of item {item_id}`, function()
			metadata.refresh(item_id, { covers = covers, bypass_cover_cache = bypass_cover_cache })
		end)
	end
	return api.response.empty(204)
end

function module.register()
	api.post(P .. "/Items/{itemId}/Refresh", module.handle_refresh_item, { auth = "public", case_insensitive = true })
end

return module
