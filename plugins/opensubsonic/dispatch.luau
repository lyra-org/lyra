--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local auth_protocol = require("./protocol/auth")
local browsing = require("./endpoints/browsing")
local lists = require("./endpoints/lists")
local media = require("./endpoints/media")
local playlists = require("./endpoints/playlists")
local request = require("./protocol/request")
local response = require("./protocol/response")
local search = require("./endpoints/search")
local system = require("./endpoints/system")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type Request = request.Request
type Method = (req: Request) -> ApiResponse

local xml_errors: { [string]: boolean } = media.xml_errors

local methods: { [string]: Method } = {}
for _, family: { [string]: Method } in
	{ system.methods, browsing.methods, lists.methods, search.methods, media.methods, playlists.methods } :: { { [string]: Method } }
do
	for name, method in family do
		methods[name] = method
	end
end

local module = {}

-- Answers `/rest/{method}`, with or without the `.view` suffix.
function module.dispatch(ctx: ApiContext): ApiResponse
	local params = request.params(ctx)
	local name = string.gsub(ctx.params.method, "%.view$", "")

	local public_method = system.public_methods[name]
	if public_method then
		return public_method(params)
	end

	local method = methods[name]
	if not method then
		return response.failed(params, response.error(0, `Unknown method: {name}`), 404)
	end

	local function failed(err: response.SubsonicError): ApiResponse
		return if xml_errors[name] then response.failed_xml(err) else response.failed(params, err)
	end

	for _, required in { "v", "c" } do
		if not request.param(params, required) then
			return failed(response.error(10, `Required parameter is missing: {required}`))
		end
	end

	local resolved, err = auth_protocol.authenticate(params)
	if not resolved then
		return failed(err :: response.SubsonicError)
	end

	local ok, result = pcall(method, { ctx = ctx, params = params, auth = resolved })
	if ok then
		return result
	end
	local failure = request.failure_of(result)
	if failure then
		return failed(failure)
	end
	warn(`{name} failed: {tostring(result)}`)
	return failed(response.error(0, "Internal server error"))
end

return module
