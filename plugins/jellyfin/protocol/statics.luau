--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")

type ApiResponse = api.ApiResponse
type JsonValue = api.JsonValue
type JsonObject = { [string]: JsonValue }

local module = {}

local EMPTY_LIST: { JsonValue } = {}

local BRANDING_CONFIG: JsonObject = {
	LoginDisclaimer = "",
	CustomCss = "",
	SplashscreenEnabled = false,
	SplashscreenLocation = "",
}

function module.utc_time_response(): ApiResponse
	local iso_now = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time())
	return api.response.json(200, {
		RequestReceptionTime = iso_now,
		ResponseTransmissionTime = iso_now,
	})
end

-- Empty on purpose: a curated stub would be misleading; clients tolerate empty arrays here.
function module.cultures_response(): ApiResponse
	return api.response.json(200, EMPTY_LIST)
end

function module.countries_response(): ApiResponse
	return api.response.json(200, EMPTY_LIST)
end

function module.parental_ratings_response(): ApiResponse
	return api.response.json(200, EMPTY_LIST)
end

function module.branding_configuration_response(): ApiResponse
	return api.response.json(200, BRANDING_CONFIG)
end

function module.branding_css_response(): ApiResponse
	return api.response.text(200, "", { ["content-type"] = "text/css; charset=utf-8" })
end

function module.image_not_found_response(): ApiResponse
	return api.response.empty(404)
end

return module
