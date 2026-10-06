local apply = require("../../resolution/apply")
local musicbrainz_api = require("../../resolution/musicbrainz_api")

local unmatched: { number } = {}
apply.set_provider({
	mark_unmatched = function(_self: any, db_id: number, _id_types: { string })
		table.insert(unmatched, db_id)
	end,
} :: any)

-- Runs a refresh-shaped call on a fresh thread, as each refresh handler gets:
-- one request answered as `pcall(http.request, ...)` would, then an unmatch.
local function unmatches_after(ok: boolean, response: any): boolean
	table.clear(unmatched)
	local resumed, err = coroutine.resume(coroutine.create(function()
		musicbrainz_api.response_object(ok, response)
		apply.mark_unmatched(1, { "release_id" })
	end))
	assert(resumed, tostring(err))
	return #unmatched > 0
end

local function status(code: number, body: string): any
	return { success = code == 200, status_code = code, body = body, retries = 0 }
end

assert(not unmatches_after(false, "connection refused"), "a transport error keeps the match")
assert(not unmatches_after(true, status(503, "")), "a server error keeps the match")
assert(not unmatches_after(true, status(200, "not json")), "an unreadable body keeps the match")

assert(unmatches_after(true, status(404, "")), "a 404 is an answer")
assert(unmatches_after(true, status(200, '{"count": 0, "artists": []}')), "an empty result is an answer")
