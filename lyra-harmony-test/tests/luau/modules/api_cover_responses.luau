local api = require("@lyra/api")

local function refuses(label: string, expected: string, build: () -> any)
	local ok, err = pcall(build)
	assert(not ok, `{label} built`)
	assert(string.find(tostring(err), expected, 1, true), `{label}: {tostring(err)}`)
end

local cover = api.response.cover("cover", { format = "webp", quality = 80, max_width = 300, max_height = 200 })
assert(cover.kind == "cover" and cover.cover_id == "cover")
assert(cover.options.format == "webp" and cover.options.quality == 80)
assert(cover.options.max_width == 300 and cover.options.max_height == 200)
local stored = api.response.cover("cover")
assert(stored.kind == "cover" and stored.options == nil, "no options serve the stored image")
assert(api.response.cover("cover", {}).kind == "cover", "empty options")
assert(api.response.cover("cover", { quality = 0, max_width = 4294967295 }).kind == "cover", "the widest bounds")
-- A bound past the widest the host takes is capped rather than refused.
assert(api.response.cover("cover", { max_width = 4294967296, max_height = 1e300 }).kind == "cover", "huge bounds")

-- A status and headers set on the response apply to the image Lyra serves.
stored.status = 203
stored.headers = { etag = '"tag"' }
assert(api.response.with_fallback(stored, api.response.empty(404)).fallback ~= nil, "a cover takes a fallback")

refuses(
	"an unknown option",
	"cover options: unknown field 'width'; expected format, quality, max_width, max_height",
	function()
		return api.response.cover("cover", { width = 300 } :: any)
	end
)
refuses("a format covers aren't served in", `cover options.format must be one of "jpg", "png", "webp"`, function()
	return api.response.cover("cover", { format = "gif" } :: any)
end)
refuses("a format in another case", `got "JPG"`, function()
	return api.response.cover("cover", { format = "JPG" } :: any)
end)
refuses("a quality over 100", "cover options: quality must be between 0 and 100", function()
	return api.response.cover("cover", { quality = 101 })
end)
refuses("a zero bound", "cover options: max_width must be greater than 0", function()
	return api.response.cover("cover", { max_width = 0 })
end)
refuses("a negative bound", "cover options.max_height must be a non-negative integer, got -1", function()
	return api.response.cover("cover", { max_height = -1 })
end)
refuses("a fractional bound", "cover options.max_width must be an integer number, got 1.5", function()
	return api.response.cover("cover", { max_width = 1.5 })
end)
refuses("options that aren't a table", "cover options must be a table, got string", function()
	return api.response.cover("cover", "webp" :: any)
end)
