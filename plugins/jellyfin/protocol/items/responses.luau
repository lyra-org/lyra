--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

local module = {}

function module.bad_request(message: string?): api.ApiResponse
	return api.response.json(400, {
		Message = message or "Bad Request",
	})
end

function module.not_found(message: string?): api.ApiResponse
	return api.response.json(404, {
		Message = message or "Not Found",
	})
end

function module.forbidden(message: string?): api.ApiResponse
	return api.response.json(403, {
		Message = message or "Forbidden",
	})
end

return module
