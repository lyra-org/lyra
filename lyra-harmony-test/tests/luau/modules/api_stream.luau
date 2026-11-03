local _, harness = ...
local api = require("@lyra/api")

assert(api.MAX_STREAM_CHUNK_BYTES == 4 * 1024 * 1024, `MAX_STREAM_CHUNK_BYTES {api.MAX_STREAM_CHUNK_BYTES}`)

local function respond(produce: (api.ApiStreamWriter) -> (), headers: api.ApiHeaders?): any
	return harness.plugin:call(function()
		return api.response.stream(200, produce, headers)
	end)
end

local response = respond(function(writer)
	writer:write("one,")
	writer:write("")
	writer:write("two")
end, { ["content-type"] = "text/csv" })
assert(response.kind == "stream" and response.status == 200, `{response.kind} {response.status}`)
assert(response.headers["content-type"] == "text/csv", "the content type is the plugin's")
assert(harness.plugin:read_body(response) == "one,two")

local default = respond(function(writer)
	writer:write("x")
end)
assert(default.headers["content-type"] == "application/octet-stream")
assert(harness.plugin:read_body(default) == "x")

-- A producer that fails, or returns after a write raised, never ends the body whole.
local function body_error(produce: (api.ApiStreamWriter) -> ()): string
	local ok, err = pcall(function()
		return harness.plugin:read_body(respond(produce))
	end)
	assert(not ok, "the body ended whole")
	return tostring(err)
end

local failed = body_error(function(writer)
	writer:write("partial")
	error("the producer failed")
end)
assert(string.find(failed, "the producer failed", 1, true), failed)

local oversized = body_error(function(writer)
	local ok, err = pcall(writer.write, writer, string.rep("x", api.MAX_STREAM_CHUNK_BYTES + 1))
	assert(not ok and string.find(tostring(err), "MAX_STREAM_CHUNK_BYTES", 1, true), tostring(err))
end)
assert(string.find(oversized, "after a failed write", 1, true), oversized)

local not_bytes = body_error(function(writer)
	pcall(writer.write, writer, 42 :: any)
end)
assert(string.find(not_bytes, "after a failed write", 1, true), not_bytes)

local ok, err = pcall(harness.plugin.read_body, harness.plugin, api.response.text(200, "x"))
assert(not ok and string.find(tostring(err), "takes a stream response", 1, true), tostring(err))
