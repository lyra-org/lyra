local fixture = ...

local constants = require("../../constants")
local images = require("../../routes/images")
local item_ids = require("../../protocol/items/identifiers")
local session = require("../../protocol/session")

constants.configure_root_item_id(string.rep("1", 32))

local album = item_ids.encode(fixture.ids.album) :: string
local AUTHORIZED = { authorization = `MediaBrowser Token="{fixture.api_keys.phone}"` }

local function context(params: { [string]: string }, query: { [string]: string }?, headers: { [string]: string }?): any
	local raw: { [string]: { string } } = {}
	for key, value in query or {} do
		raw[key] = { value }
	end
	return {
		plugin_id = "jellyfin",
		request = { method = "GET", path = "/", headers = headers or {}, query = raw, body_raw = "" },
		params = params,
	}
end

local function outcome(response: any): number | string
	return if response.kind == "cover" then "cover" else response.status
end

-- Item routes take the index from the route, else from the query.
local function primary(index: string?, query: { [string]: string }?): number | string
	return outcome(session.handle_primary_image_request(context({ imageId = album, imageIndex = index }, query)))
end
assert(primary(nil) == "cover")
assert(primary("0") == "cover", "the first image by the route")
assert(primary("1") == 404, "Lyra keeps one image")
assert(primary("-1") == 404, "a negative index binds, and names no image")
assert(primary("+0") == "cover", "a signed zero is the first image")
assert(primary("-0") == "cover")
assert(primary("+1") == 404)
assert(primary("-2147483648") == 404, "the smallest int")
assert(primary("-2147483649") == 400, "past the smallest int")
assert(primary("2147483648") == 400, "past the largest int")
assert(primary("+") == 400, "a sign alone")
assert(primary("x") == 400, "a non-integer index")
assert(primary("1.5") == 400, "a fractional index")
assert(primary("99999999999") == 400, "past what upstream's int holds")
assert(primary(nil, { imageIndex = "0" }) == "cover", "the first image by the query")
assert(primary(nil, { imageIndex = "2" }) == 404)
assert(primary(nil, { imageIndex = "nope" }) == 400)
assert(primary("0", { imageIndex = "2" }) == "cover", "the route wins over the query")

-- Named images take the same index.
local function genre_or_person(query: { [string]: string }?): number
	return (session.handle_person_image_request(context({ name = "nobody", imageType = "Primary" }, query)) :: any).status
end
assert(genre_or_person({ imageIndex = "bad" }) == 400)
assert(genre_or_person({ imageIndex = "3" }) == 404)

-- The long form takes what the short one's query gives from its path.
local function path_request(overrides: { [string]: string }?, query: { [string]: string }?): any
	local params: { [string]: string } = {
		imageId = album,
		imageIndex = "0",
		tag = "abc",
		format = "webp",
		maxWidth = "300",
		maxHeight = "0",
		percentPlayed = "0",
		unplayedCount = "0",
	}
	for key, value in overrides or {} do
		params[key] = value
	end
	return session.handle_primary_image_path_request(context(params, query))
end

local served = path_request()
assert(served.kind == "cover", "the long form serves the cover")
assert(served.options.format == "webp" and served.options.max_width == 300, "format and bound from the path")
assert(served.options.max_height == nil, "a zero bound is none")
local sized = path_request(nil, { MaxWidth = "50", MaxHeight = "60", fillWidth = "200", fillHeight = "80" }).options
assert(sized.max_width == 300, "the path's maximum wins over the query's sizes")
assert(sized.max_height == 80, "a zero maximum leaves the query's sizes, never its maximum")
assert(served.headers.etag == '"abc"' and served.headers["cache-control"] == "public, max-age=31536000, immutable")
assert(path_request({ imageIndex = "1" }).status == 404)
assert(path_request({ format = "gif" }).status == 400, "an unknown format")
assert(path_request({ maxWidth = "wide" }).status == 400, "a bound that isn't a number")
assert(path_request({ percentPlayed = "nope" }).status == 400, "an overlay that isn't a number")
assert(path_request({ imageId = item_ids.encode(fixture.ids.plain) :: string }).status == 404, "no cover")

-- A user image is never there, once the request can be read.
local function user_image(query: { [string]: string }?, headers: { [string]: string }?): any
	return session.handle_user_image_request(context({}, query, headers))
end
local listener = item_ids.encode(fixture.users.listener) :: string
assert(user_image({ userId = "nope" }).status == 400, "an unreadable id")
assert(user_image().status == 400, "nobody named and nobody signed in")
assert(user_image({ userId = item_ids.EMPTY_GUID }).status == 400, "the empty id names nobody")
assert(
	user_image({ userId = item_ids.EMPTY_GUID }, AUTHORIZED).status == 400,
	"only leaving the id out falls back to the caller"
)
assert(user_image(nil, AUTHORIZED).status == 404, "the caller, without an image")
assert(user_image({ userId = listener }).status == 404, "any user can be asked for")

local function user_images(user_id: string, index: string?): number
	local params: { [string]: string } = { userId = user_id, imageType = "Primary", imageIndex = index }
	return (session.handle_user_images_route_request(context(params)) :: any).status
end
assert(user_images("nope") == 400)
assert(user_images(item_ids.EMPTY_GUID) == 400, "the empty id, as `UserImage` refuses it")
assert(user_images(listener) == 404)
assert(user_images(listener, "0") == 404)
assert(user_images(listener, "bad") == 400)

-- An item's image list needs a signed-in caller who can see the item.
local function image_infos(id: string, headers: { [string]: string }?): any
	return images.handle_item_image_infos(context({ imageId = id }, nil, headers or AUTHORIZED))
end
assert(image_infos(album, {}).status == 401, "anonymous")
assert(image_infos(item_ids.encode(fixture.ids.secret) :: string).status == 404, "an item the caller can't see")
assert(image_infos("00000000000000000000000000000abc").status == 404, "an unknown item")
assert(image_infos(item_ids.encode(fixture.ids.plain) :: string).status == 200, "an item without an image")
assert(#image_infos(item_ids.encode(fixture.ids.plain) :: string).body == 0, "lists nothing")
assert(#image_infos(constants.ROOT_ITEM_ID).body == 0, "the root has no image")
assert(#image_infos(constants.PLAYLISTS_FOLDER_ID).body == 0, "nor the playlists view")

local listed = image_infos(album)
assert(listed.status == 200 and #listed.body == 1, "one image")
local info = listed.body[1]
assert(info.ImageType == "Primary" and type(info.ImageTag) == "string" and info.ImageTag ~= "")
assert(info.Size == 0 and info.BlurHash == "L00000fQfQfQ", `size {info.Size}, blurhash {info.BlurHash}`)
assert(info.Path == nil and info.ImageIndex == nil and info.Width == nil and info.Height == nil)

local track_infos = image_infos(item_ids.encode(fixture.ids.song) :: string)
assert(#track_infos.body == 1, "a track shows the image it serves")
