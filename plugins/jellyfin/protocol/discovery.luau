--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local json = require("@harmony/json")
local net = require("@harmony/net")
local task = require("@harmony/task")

local constants = require("../constants")

type ServerInfo = {
	port: number,
}

local module = {}

local DISCOVERY_MESSAGE = "who is jellyfinserver?"
local DISCOVERY_RATE_LIMIT_MS = 100

local function is_private_ipv4(ip: string): boolean
	if string.sub(ip, 1, 3) == "10." then
		return true
	end
	if string.sub(ip, 1, 4) == "172." then
		local second = tonumber(string.match(ip, "^172%.(%d+)%."))
		if second and second >= 16 and second <= 31 then
			return true
		end
	end
	if string.sub(ip, 1, 8) == "192.168." then
		return true
	end
	if string.sub(ip, 1, 8) == "169.254." then
		return true
	end
	if string.sub(ip, 1, 4) == "127." then
		return true
	end
	return false
end

function module.start(server_info: ServerInfo)
	if not constants.DISCOVERY_ENABLED then
		return
	end

	task.spawn(function()
		local bind_ok, bind_result =
			pcall(net.udp_bind, { address = "0.0.0.0", port = constants.DISCOVERY_PORT } :: net.UdpBindOptions)
		if not bind_ok then
			print("auto-discovery unavailable: " .. tostring(bind_result))
			return
		end
		local socket = bind_result :: net.UdpSocket

		local last_response: { [string]: number } = {}
		local last_eviction = os.clock()
		local EVICTION_INTERVAL = 30
		local EVICTION_MAX_AGE_MS = 10000

		while true do
			local recv_ok, data, sender = pcall(socket.recv_from, socket, nil)
			if not recv_ok then
				task.wait(1)
				continue
			end

			local sender_ip = (sender :: net.SocketAddress).address
			if not is_private_ipv4(sender_ip) then
				continue
			end

			if string.lower(tostring(data)) ~= DISCOVERY_MESSAGE then
				continue
			end

			local now = os.clock() * 1000
			local last = last_response[sender_ip]
			if last and (now - last) < DISCOVERY_RATE_LIMIT_MS then
				continue
			end
			last_response[sender_ip] = now

			local now_seconds = now / 1000
			if now_seconds - last_eviction > EVICTION_INTERVAL then
				for ip, ts in last_response do
					if now - ts > EVICTION_MAX_AGE_MS then
						last_response[ip] = nil
					end
				end
				last_eviction = now_seconds
			end

			local probe_ok, probe_result = pcall(net.udp_bind, { address = "0.0.0.0", port = 0 } :: net.UdpBindOptions)
			if not probe_ok then
				continue
			end
			local probe = probe_result :: net.UdpSocket
			local connect_ok, local_ip = pcall(function()
				probe:connect(sender :: net.SocketAddress)
				return probe:address().address
			end)
			probe:close()
			if not connect_ok then
				continue
			end

			local address = "http://" .. local_ip .. ":" .. tostring(server_info.port) .. constants.ROUTE_PREFIX
			local response = json.encode({
				Address = address,
				Id = constants.SERVER_ID,
				Name = "Lyra",
			})

			pcall(socket.send_to, socket, response, sender :: net.SocketAddress, nil)
		end
	end)
end

return module
