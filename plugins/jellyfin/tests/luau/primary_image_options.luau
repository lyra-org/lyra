local fixture = ...
local covers = require("@lyra/covers")

local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

local album = fixture.ids.album
local cover = covers.get({ album })[album]

local function primary_image(query: { [string]: string }, headers: { [string]: string }?): any
	local params: { [string]: { string } } = {}
	for key, value in query do
		params[key] = { value }
	end
	return session.handle_primary_image_request({
		plugin_id = "jellyfin",
		request = { method = "GET", path = "/", headers = headers or {}, query = params, body_raw = "" },
		params = { imageId = item_ids.encode_jellyfin_item_id(album) },
	} :: any)
end

local function options(query: { [string]: string }): any
	local response = primary_image(query)
	assert(response.kind == "cover", `answered {response.kind}`)
	return response.options or {}
end

-- The host serves the stored image, in its own format, and fills in the defaults.
local stored = primary_image({})
assert(stored.kind == "cover" and stored.cover_id == cover.id, "serves the item's cover by its id")
assert(stored.options.format == nil and stored.options.quality == nil, "no format or quality filled in")
assert(stored.options.max_width == nil and stored.options.max_height == nil)
assert(stored.headers["cache-control"] == "public" and stored.headers["content-type"] == nil)

local asked = options({ format = "WEBP", quality = "80", maxwidth = "300", MaxHeight = "200" })
assert(asked.format == "webp" and asked.quality == 80, `format {asked.format}, quality {asked.quality}`)
assert(asked.max_width == 300 and asked.max_height == 200, "keys in any case")

-- An explicit maximum wins; otherwise the smallest of the fill and fixed sizes bounds the side.
local filled = options({ fillWidth = "400", width = "250", fillHeight = "180" })
assert(filled.max_width == 250 and filled.max_height == 180, `{filled.max_width}x{filled.max_height}`)
local capped = options({ MaxWidth = "500", fillWidth = "100", Height = "320" })
assert(capped.max_width == 500 and capped.max_height == 320, `{capped.max_width}x{capped.max_height}`)
local beyond = options({ fillWidth = "99999999999", width = "0", MaxHeight = "1e999" })
assert(beyond.max_width == 99999999999, "the host caps a size past its own bound")
assert(beyond.max_height == nil, "a size that isn't a number bounds nothing")

assert(primary_image({ format = "gif" }).status == 400, "an unknown format is refused")

-- A tagged request is cached for good and validated by its tag.
local tagged = primary_image({ tag = cover.hash })
assert(tagged.headers.etag == `"{cover.hash}"`, tagged.headers.etag)
assert(tagged.headers["cache-control"] == "public, max-age=31536000, immutable")
local unchanged = primary_image({ tag = cover.hash }, { ["if-none-match"] = `"{cover.hash}"` })
assert(unchanged.kind == "empty" and unchanged.status == 304, "an unchanged image is not sent again")
local uncached = primary_image({}, { ["cache-control"] = "no-cache" })
assert(uncached.headers["cache-control"] == "no-cache, no-store, must-revalidate")
