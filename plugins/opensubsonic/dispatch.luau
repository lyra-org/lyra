--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local annotation = require("./endpoints/annotation")
local auth_protocol = require("./protocol/auth")
local bookmarks = require("./endpoints/bookmarks")
local browsing = require("./endpoints/browsing")
local lists = require("./endpoints/lists")
local media = require("./endpoints/media")
local playlists = require("./endpoints/playlists")
local request = require("./protocol/request")
local response = require("./protocol/response")
local scanning = require("./endpoints/scanning")
local search = require("./endpoints/search")
local sonic = require("./endpoints/sonic")
local system = require("./endpoints/system")
local transcoding = require("./endpoints/transcoding")
local users = require("./endpoints/users")

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse
type Request = request.Request
type Method = (req: Request) -> ApiResponse

-- A family of methods, with those that answer every error in XML or as a bare HTTP error.
type Family = {
	methods: { [string]: Method },
	xml_errors: { [string]: boolean }?,
	http_errors: { [string]: boolean }?,
}

local methods: { [string]: Method } = {}
local xml_errors: { [string]: boolean } = {}
local http_errors: { [string]: boolean } = {}
for _, family: Family in
	{
		system,
		browsing,
		lists,
		search,
		media,
		playlists,
		scanning,
		annotation,
		users,
		bookmarks,
		sonic,
		transcoding,
	} :: { Family }
do
	for name, method in family.methods do
		methods[name] = method
	end
	for name in family.xml_errors or {} do
		xml_errors[name] = true
	end
	for name in family.http_errors or {} do
		http_errors[name] = true
	end
end

local module = {}

-- What method `name` answers, given how its protected call ended: its result, or the Subsonic
-- error it raised, through `failed`.
local function answered(
	name: string,
	failed: (response.SubsonicError) -> ApiResponse,
	ok: boolean,
	result: any
): ApiResponse
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

-- Answers `/rest/{method}`, with or without the `.view` suffix; `hls` also answers as `hls.m3u8`.
function module.dispatch(ctx: ApiContext): ApiResponse
	local params = request.params(ctx)
	local name = string.gsub(ctx.params.method, "%.view$", "")
	if name == "hls.m3u8" then
		name = "hls"
	end

	local public_method = system.public_methods[name]
	if public_method then
		return answered(name, function(err: response.SubsonicError): ApiResponse
			return response.failed(params, err)
		end, pcall(public_method, params))
	end

	local method = methods[name]
	if not method then
		return response.unknown_method(params, name)
	end

	local function failed(err: response.SubsonicError): ApiResponse
		if http_errors[name] then
			return response.failed_http(err)
		end
		return if xml_errors[name] then response.failed_xml(err) else response.failed(params, err)
	end

	for _, required in { "v", "c" } do
		if not request.param(params, required) then
			return failed(response.error(10, `Required parameter is missing: {required}`))
		end
	end

	local authenticated, resolved_or_error, err = pcall(auth_protocol.authenticate, params)
	if not authenticated then
		warn(`{name} authentication failed: {tostring(resolved_or_error)}`)
		return failed(response.error(0, "Internal server error"))
	end
	local resolved = resolved_or_error :: auth_protocol.ResolvedAuth?
	if not resolved then
		return failed(err :: response.SubsonicError)
	end

	return answered(name, failed, pcall(method, { ctx = ctx, params = params, auth = resolved }))
end

return module
