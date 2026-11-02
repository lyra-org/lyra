--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

-- What the plugin checks with AudioMuse as it loads.

local client = require("./client")

local startup = {}

-- How long the check waits, so an unreachable AudioMuse doesn't hold up loading.
local CHECK_TIMEOUT_MS = 5000

-- Warns when AudioMuse rejects the configured token or server, and returns whether the check
-- passed.
function startup.check(): boolean
	local response = client.get("/api/models", nil, CHECK_TIMEOUT_MS)
	local body = if client.ok(response) then client.decode(response.body) else nil
	if type(body) ~= "table" then
		warn(`AudioMuse check failed: {client.error_message(response)}`)
		return false
	end
	return true
end

return startup
