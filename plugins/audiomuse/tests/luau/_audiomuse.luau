--!strict
-- Answers the plugin's AudioMuse requests with scripted responses, and records them.
local http = require("@harmony/http")
local serde = require("@harmony/serde")

local client = require("../../client")
local jellyfin_hex = require("../../jellyfin_hex")
local mix = require("@lyra/mix")
local mixes = require("../../mixes")

local module = {}

module.BASE_URL = "http://audiomuse.test"

-- A request as sent: its method, its URL, and its JSON body, raw and decoded.
export type Request = { method: string, url: string, raw: string?, body: any }
-- A route answers each request whose URL contains its pattern with its status (200 by default)
-- and its body, encoded as JSON.
export type Route = { pattern: string, status: number?, body: any }

module.routes = {} :: { Route }
module.requests = {} :: { Request }

client.send = function(options: http.HttpRequestOptions): http.HttpResponse
	local body = if type(options.body) == "string" then serde.json.decode(options.body) else nil
	local method = if options.method == http.HttpMethod.Get
		then "GET"
		elseif options.method == http.HttpMethod.Post then "POST"
		else "OTHER"
	local raw = if type(options.body) == "string" then options.body else nil
	table.insert(module.requests, { method = method, url = options.url, raw = raw, body = body })
	for _, route in module.routes do
		if string.find(options.url, route.pattern, 1, true) then
			local status = route.status or 200
			return {
				-- As harmony's HTTP client reports it: a response arrived, whatever its status.
				success = true,
				status_code = status,
				status_message = "",
				headers = {},
				cookies = {},
				body = serde.json.encode(route.body),
				retries = 0,
				rate_limited = false,
			}
		end
	end
	error(`unexpected request {options.url}`)
end

function module.configure()
	client.configure({ base_url = module.BASE_URL, token = "secret" })
end

-- Registers the AudioMuse mixer as the plugin does with Lyra seen through Jellyfin.
function module.register()
	module.configure()
	mixes.register(mix.Mixer.new("audiomuse"), {
		ids = { encode = jellyfin_hex.nanoid_to_hex, decode = jellyfin_hex.hex_to_nanoid },
	})
end

-- The item id AudioMuse knows a Lyra track by.
function module.item(track_id: string): string
	return assert(jellyfin_hex.nanoid_to_hex(track_id))
end

-- AudioMuse result rows for the tracks, in order.
function module.rows(...: string): { { item_id: string } }
	local rows = {}
	for _, track_id in { ... } do
		table.insert(rows, { item_id = module.item(track_id) })
	end
	return rows
end

-- Forgets the recorded requests and returns the ones made so far.
function module.take(): { Request }
	local taken = module.requests
	module.requests = {}
	return taken
end

-- The query parameter `name` of a URL, decoded.
function module.param(url: string, name: string): string?
	local value: string? = string.match(url, `[?&]{name}=([^&]*)`)
	if not value then
		return nil
	end
	return (
		string.gsub(value, "%%(%x%x)", function(hex: string): string
			return string.char(tonumber(hex, 16) :: number)
		end)
	)
end

function module.titles(tracks: { mix.Track }?): string
	assert(tracks, "expected a mix")
	local names = {}
	for _, track in tracks do
		table.insert(names, track.track_title)
	end
	return table.concat(names, ",")
end

return module
