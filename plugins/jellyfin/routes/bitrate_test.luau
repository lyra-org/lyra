--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- Upstream's `/Playback/BitrateTest`: `Size` bytes of filler a client times to measure its bitrate.

local api = require("@lyra/api")

local constants = require("../constants")
local requests = require("../protocol/items/requests")
local responses = require("../protocol/items/responses")
local session = require("../protocol/session")

local P = constants.ROUTE_PREFIX

type ApiContext = api.ApiContext
type ApiResponse = api.ApiResponse

local module = {}

local DEFAULT_SIZE = 102400
local MIN_SIZE = 1
local MAX_SIZE = 100000000
local SIZE_ERROR = `The requested size must be greater than or equal to {MIN_SIZE} and less than or equal to {MAX_SIZE}`

local function parse_size(ctx: ApiContext): (number?, ApiResponse?)
	local query = if ctx and ctx.request then ctx.request.query else nil
	local raw_size = requests.parse_query_string(query, { "Size", "size" }, nil)
	if raw_size == nil then
		return DEFAULT_SIZE, nil
	end

	local size = tonumber(requests.trim(raw_size) or "")
	if size == nil or size ~= size or math.floor(size) ~= size or size < MIN_SIZE or size > MAX_SIZE then
		return nil, responses.bad_request(SIZE_ERROR)
	end
	return size, nil
end

function module.handle(ctx: ApiContext): ApiResponse
	local principal = session.require_jellyfin_principal(ctx)
	if not principal then
		return session.unauthorized()
	end

	local size, size_err = parse_size(ctx)
	if size == nil then
		return size_err or responses.bad_request(SIZE_ERROR)
	end
	return api.response.filler(200, size)
end

function module.register()
	api.get(P .. "/Playback/BitrateTest", module.handle, { auth = "public", case_insensitive = true })
end

return module
