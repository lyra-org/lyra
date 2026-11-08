--!strict
-- This Source Code Form is subject to the terms of the Lyra Public License,
-- v1.0. If a copy of the Lyra Public License was not distributed with this file,
-- You can obtain one here:
-- www.meshiplaw.com/lyra.

local api = require("@lyra/api")
local server = require("@lyra/server")

local constants = require("../constants")

type ApiContext = api.ApiContext
type ServerInfo = server.ServerInfo

export type PublicSystemInfo = {
	ServerName: string,
	Version: string,
	ProductName: string,
	Id: string,
	LocalAddress: string?,
	StartupWizardCompleted: boolean,
}

local module = {}

-- Represents the last known supported Jellyfin server version.
local SERVER_VERSION = "12.4.0"

function module.public_system_info<T>(server_info: ServerInfo, ctx: ApiContext, extra: T): T & PublicSystemInfo
	local base_url = server_info.published_url or ctx.request.base_url
	local info: T & PublicSystemInfo = extra
	info.ServerName = "Lyra"
	info.Version = SERVER_VERSION
	-- Official Jellyfin SDK server discovery rejects servers whose product name is not "Jellyfin Server".
	info.ProductName = "Jellyfin Server"
	info.Id = constants.SERVER_ID
	info.LocalAddress = if base_url then `{base_url}{constants.ROUTE_PREFIX}` else nil
	info.StartupWizardCompleted = true
	return info
end

return module
