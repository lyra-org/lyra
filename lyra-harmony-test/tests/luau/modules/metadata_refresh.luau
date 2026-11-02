local fixture = ...
local metadata = require("@lyra/metadata")

local ids = fixture.ids
local refreshed: { string } = {}
local provider = metadata.Provider.new("refresh-test")
provider:refresh(metadata.EntityType.Release, function(ctx: metadata.ReleaseRefreshContext)
	table.insert(refreshed, ctx.id :: string)
	return nil
end)

-- A refresh returns once the providers have answered, naming them.
local result = metadata.refresh(ids.album)
assert(result ~= nil and result.entity_type == "release", "the release is refreshed")
assert(#result.provider_ids == 1 and result.provider_ids[1] == "refresh-test")
assert(#refreshed == 1 and refreshed[1] == ids.album, "the provider was called before the refresh returned")

-- Options pass through; a track or artist without a provider for it is refreshed by none.
result = metadata.refresh(ids.album, { replace_cover = true, force_refresh = true, provider_options = {} })
assert(result ~= nil and #refreshed == 2)
local track = metadata.refresh(ids.song)
assert(track ~= nil and track.entity_type == "track" and #track.provider_ids == 0)
local artist = metadata.refresh(ids.singer)
assert(artist ~= nil and artist.entity_type == "artist" and #artist.provider_ids == 0)

-- Without covers, the providers still run.
local metadata_only = metadata.refresh(ids.album, { covers = false })
assert(metadata_only ~= nil and #refreshed == 3)
assert(not pcall(metadata.refresh, ids.album, { covers = false, force_refresh = true }))

-- What is missing, hidden or of another type is nil.
assert(metadata.refresh(ids.secret) == nil, "a release in a hidden library")
assert(metadata.refresh("missing") == nil)
assert(metadata.refresh(ids.mine) == nil, "a playlist has no metadata to refresh")
assert(#refreshed == 3, "nothing else reached the provider")

-- Malformed options raise.
assert(not pcall(metadata.refresh, ids.album, { replace = true } :: any))
assert(not pcall(metadata.refresh, ids.album, { provider_options = { mode = 1 } } :: any))
