--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local auth = require("@lyra/auth")

type ApiContext = api.ApiContext
type ApiQueryParams = api.ApiQueryParams
type ResolvedAuth = auth.ResolvedAuth

export type SubsonicError = {
	code: number,
	message: string,
	helpUrl: string?,
}

export type Request = {
	ctx: ApiContext,
	params: ApiQueryParams,
	auth: ResolvedAuth,
}

type Failure = { subsonic_error: SubsonicError }

local FORM_MEDIA_TYPE = "application/x-www-form-urlencoded"

local module = {}

local function header(ctx: ApiContext, name: string): string?
	for key, value in ctx.request.headers do
		if string.lower(key) == name then
			return value
		end
	end
	return nil
end

local function decode_form_component(value: string): string
	local spaced = string.gsub(value, "%+", " ")
	return (
		string.gsub(spaced, "%%(%x%x)", function(hex: string): string
			return string.char(tonumber(hex, 16) :: number)
		end)
	)
end

-- The query parameters, followed by the fields of a form-encoded POST body.
function module.params(ctx: ApiContext): ApiQueryParams
	local query = ctx.request.query
	local content_type = header(ctx, "content-type")
	if string.upper(ctx.request.method) ~= "POST" or not content_type then
		return query
	end
	local media_type = string.match(content_type, "^%s*([^;%s]+)")
	if not media_type or string.lower(media_type) ~= FORM_MEDIA_TYPE then
		return query
	end

	local params: ApiQueryParams = {}
	for name, values in query do
		params[name] = table.clone(values)
	end

	for pair in string.gmatch(ctx.request.body_raw, "[^&]+") do
		local raw_name, raw_value = string.match(pair, "^([^=]*)=?(.*)$")
		local name = decode_form_component(raw_name or "")
		if name ~= "" then
			local values = params[name] or {}
			table.insert(values, decode_form_component(raw_value or ""))
			params[name] = values
		end
	end
	return params
end

-- A parameter's first value, or nil when it is absent or empty.
function module.param(params: ApiQueryParams, name: string): string?
	local value = module.param_or_empty(params, name)
	return if value ~= "" then value else nil
end

-- A parameter's first value, which may be empty, or nil when it is absent.
function module.param_or_empty(params: ApiQueryParams, name: string): string?
	local values = params[name]
	return if values then values[1] else nil
end

-- Ends the method with a Subsonic error, which the dispatcher answers with.
function module.fail(code: number, message: string): ...any
	local failure: Failure = { subsonic_error = { code = code, message = message } }
	error(failure, 0)
end

function module.failure_of(value: unknown): SubsonicError?
	if type(value) == "table" then
		local failure = (value :: any).subsonic_error
		if type(failure) == "table" then
			return failure :: SubsonicError
		end
	end
	return nil
end

function module.not_found(what: string): ...any
	return module.fail(70, `{what} not found`)
end

function module.required(params: ApiQueryParams, name: string): string
	local value = module.param(params, name)
	if not value then
		return module.fail(10, `Required parameter is missing: {name}`)
	end
	return value
end

-- Every non-empty value of a repeatable parameter, in order.
function module.values(params: ApiQueryParams, name: string): { string }
	local values: { string } = {}
	for _, value in params[name] or {} do
		if value ~= "" then
			table.insert(values, value)
		end
	end
	return values
end

local function parse_int(name: string, value: string): number
	local number = tonumber(value)
	if not number or number ~= math.floor(number) or number ~= number or math.abs(number) == math.huge then
		return module.fail(0, `Invalid integer for {name}: {value}`)
	end
	return number
end

-- A finite number, which may have a fraction.
function module.number(params: ApiQueryParams, name: string): number?
	local value = module.param(params, name)
	if not value then
		return nil
	end
	local number = tonumber(value)
	if not number or number ~= number or math.abs(number) == math.huge then
		return module.fail(0, `Invalid number for {name}: {value}`)
	end
	return number
end

function module.int(params: ApiQueryParams, name: string): number?
	local value = module.param(params, name)
	return if value then parse_int(name, value) else nil
end

function module.ints(params: ApiQueryParams, name: string): { number }
	local numbers: { number } = {}
	for _, value in module.values(params, name) do
		table.insert(numbers, parse_int(name, value))
	end
	return numbers
end

-- A count or offset: absent gives `default`, and the value is kept within `0..max`.
function module.count(params: ApiQueryParams, name: string, default: number, max: number?): number
	local value = module.int(params, name) or default
	value = math.max(value, 0)
	return if max then math.min(value, max) else value
end

function module.bool(params: ApiQueryParams, name: string): boolean?
	local value = module.param(params, name)
	if not value then
		return nil
	end
	local lowered = string.lower(value)
	if lowered == "true" then
		return true
	elseif lowered == "false" then
		return false
	end
	return module.fail(0, `Invalid boolean for {name}: {value}`)
end

return module
