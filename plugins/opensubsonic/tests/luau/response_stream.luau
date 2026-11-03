local _, harness = ...
local response = require("../../protocol/response")

local function query(params: { [string]: string })
	local result = {}
	for name, value in params do
		result[name] = { value }
	end
	return result
end

-- Lists of more items than one batch, with empty objects, text fields and escaping.
local songs = {}
for index = 1, 7 do
	table.insert(songs, {
		id = `s{index}`,
		title = `Song <{index}> & "more"`,
		genres = { { name = "Rock" } },
		replayGain = response.EMPTY_OBJECT,
	})
end
local albums = { { id = "a1", name = "Album", genre = "Rock" } }
local lists = { album = albums, song = songs }

local function streamed(params: { [string]: string }, sections: { [string]: { any } }): (any, string)
	local lists = {}
	for _, name in { "album", "song" } do
		local items = sections[name] or {}
		table.insert(lists, {
			name = name,
			each_batch = function(emit)
				for start = 1, #items, 3 do
					emit({ table.unpack(items, start, math.min(start + 2, #items)) })
				end
			end,
		})
	end
	local streamed_response = harness.plugin:call(function()
		return response.ok_stream(query(params), "searchResult3", lists)
	end)
	return streamed_response, harness.plugin:read_body(streamed_response)
end

-- A streamed document is the one `ok` answers with the same lists, byte for byte.
for _, params in { { f = "json" }, { f = "jsonp", callback = "handle" }, {} } :: { { [string]: string } } do
	for _, sections in { lists, {} } :: { { [string]: { any } } } do
		local whole = response.ok(
			query(params),
			{ searchResult3 = if next(sections) then sections else response.EMPTY_OBJECT }
		) :: any
		local stream, body = streamed(params, sections)
		assert(stream.kind == "stream" and stream.status == 200)
		assert(stream.headers["content-type"] == whole.headers["content-type"], stream.headers["content-type"])
		assert(body == whole.body, `{params.f or "xml"}: streamed\n{body}\nwhole\n{whole.body}`)
	end
end

-- A JSONP call without a valid callback fails before anything streams.
local missing = response.ok_stream(query({ f = "jsonp", callback = "1bad" }), "searchResult3", {}) :: any
assert(missing.kind == "text" and string.find(missing.body, '"code":10', 1, true), missing.body)

-- A list named twice fails before anything streams.
local twice = { name = "album", each_batch = function() end }
local ok, err = pcall(response.ok_stream, query({ f = "json" }), "searchResult3", { twice, twice })
assert(not ok and string.find(tostring(err), "list album comes twice", 1, true), tostring(err))
