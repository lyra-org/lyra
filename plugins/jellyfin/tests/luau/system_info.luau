local constants = require("../../constants")
local system_info = require("../../protocol/system_info")

local P = constants.ROUTE_PREFIX

local function info(published_url: string?, base_url: string?)
	local server_info = {
		id = "server",
		version = "1.0.0",
		commit_hash = "abc",
		port = 4746,
		tls = false,
		published_url = published_url,
		setup = { account_required = false, setup_code_required = false, plugin_selection_required = false },
	}
	local ctx = {
		plugin_id = "jellyfin",
		request = {
			method = "GET",
			path = `{P}/System/Info/Public`,
			headers = {},
			base_url = base_url,
			query = {},
			body_raw = "",
		},
		params = {},
	}
	return system_info.public_system_info(server_info :: any, ctx :: any, { Extra = true }) :: any
end

local function check(actual: any, expected: any, why: string)
	if actual ~= expected then
		error(`{why}: expected {expected}, got {actual}`)
	end
end

local published = info("https://music.example", "http://lyra.local:4746")
check(published.LocalAddress, `https://music.example{P}`, "a published URL wins over the request")

local derived = info(nil, "https://lyra.home.local")
check(derived.LocalAddress, `https://lyra.home.local{P}`, "without a published URL the request's address is used")

local neither = info(nil, nil)
check(neither.LocalAddress, nil, "LocalAddress is omitted when neither address exists")

for _, response in { published, derived, neither } do
	check(response.PublicAddress, nil, "PublicAddress is not a Jellyfin field")
	check(response.Extra, true, "the handler's own fields are kept")
	check(response.ProductName, "Jellyfin Server", "the fixed fields are set")
end
