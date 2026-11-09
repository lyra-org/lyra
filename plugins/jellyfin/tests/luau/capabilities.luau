local fixture, harness = ...
local active_sessions = require("../../protocol/active_sessions")
local capability_routes = require("../../routes/capabilities")
local datastore = require("@lyra/datastore")
local serde = require("@harmony/serde")
local session = require("../../protocol/session")
local socket_routes = require("../../routes/socket")
local syncplay = require("../../routes/syncplay")
local task = require("@harmony/task")
local websocket = require("../../protocol/websocket")

local entries = active_sessions.entries
local sockets = harness.sockets

syncplay.register(entries, function(session_id: string, principal: any): any
	return active_sessions.get_owned(session_id, principal)
end)

local AUTHORIZATION =
	`MediaBrowser Client="Finamp", Device="Pixel", DeviceId="pixel", Version="1", Token="{fixture.api_keys.phone}"`

local function context(query: any?, json: any?, authorization: string?): any
	local raw_query: { [string]: { string } } = {}
	for key, value in query or {} do
		raw_query[key] = { value }
	end
	return {
		plugin_id = "jellyfin",
		request = {
			method = "POST",
			path = "/Sessions/Capabilities",
			headers = { authorization = authorization or AUTHORIZATION },
			query = raw_query,
			body_raw = "",
			json = json,
		},
		params = {},
	}
end

local function eventually(condition: () -> boolean, why: string)
	for _ = 1, 500 do
		if condition() then
			return
		end
		harness.task:wait(0.01)
	end
	error(`timed out waiting: {why}`)
end

local function connect(authorization: string?): any
	local connection: any = { sender = sockets:open(), gone = false, done = false }
	local reader = {
		recv = function()
			while not connection.gone do
				harness.task:wait(0.02)
			end
			return nil
		end,
	}
	task.spawn(function()
		harness.plugin:call(socket_routes.handle, reader, connection.sender, context(nil, nil, authorization))
		connection.done = true
	end)
	eventually(function()
		for _, entry in entries do
			if entry.sender == connection.sender then
				connection.entry = entry
				return true
			end
		end
		return false
	end, "the socket to connect")
	return connection
end

local function disconnect(connection: any)
	connection.gone = true
	sockets:close(connection.sender)
	eventually(function()
		return connection.done
	end, "the handler to finish")
end

local function full(body: any): any
	return harness.plugin:call(capability_routes.handle, context(nil, body), true)
end
local function partial(query: any): any
	return harness.plugin:call(capability_routes.handle, context(query), false)
end

local function listed_sessions(): { any }
	local principal = harness.plugin:call(session.require_jellyfin_principal, context())
	assert(principal, "the api key authenticates")
	return active_sessions.list_visible(principal) :: any
end

local function listed_socket(connection: any): any
	for _, dto in listed_sessions() do
		if dto.Id == connection.entry.session_id then
			return dto
		end
	end
	error("the socket is listed")
end

-- An unauthenticated declaration keeps nothing.
assert(
	harness.plugin:call(
		capability_routes.handle,
		context(nil, { SupportsMediaControl = true }, 'MediaBrowser Token="x"'),
		true
	).status == 401,
	"an unknown token is refused"
)

-- Declared before the socket opens, as Finamp does, it applies when the socket connects.
assert(full({ SupportsMediaControl = true, SupportedCommands = { "Play", "SetVolume" } }).status == 204)
local connection = connect()
assert(connection.entry.capabilities.SupportsMediaControl == true, "the declaration reached the socket")
local dto = listed_socket(connection)
assert(table.concat(dto.SupportedCommands, ",") == "Play,SetVolume", "the top-level list follows it")
assert(dto.SupportsMediaControl == true and dto.SupportsRemoteControl == true)
assert(dto.UserId ~= nil and type(dto.LastActivityDate) == "string" and dto.HasCustomDeviceName == false)

-- Each declaration replaces the last, as upstream's two endpoints do.
assert(partial({ playableMediaTypes = "Audio" }).status == 204)
local replaced = connection.entry.capabilities
assert(#replaced.SupportedCommands == 0, "the query form replaces the body form")
assert(replaced.PlayableMediaTypes[1] == "Audio")
dto = listed_socket(connection)
assert(#dto.SupportedCommands == 0 and dto.SupportsMediaControl == false, "the DTO follows")
assert(dto.PlayableMediaTypes[1] == "Audio", "with the media types it declared")
assert(dto.SupportsRemoteControl == false, "and it is no cast target")

-- A declaration naming nothing still encodes as an object, with upstream's defaults.
assert(partial({}).status == 204)
local encoded = websocket.encode("Sessions", listed_sessions(), nil)
assert(not string.find(encoded, '"Capabilities":[', 1, true), encoded)
local pushed = serde.json.decode(encoded).Data[1]
assert(pushed.Capabilities.SupportsMediaControl == false and pushed.Capabilities.SupportsPersistentIdentifier == true)
assert(#pushed.Capabilities.SupportedCommands == 0 and #pushed.Capabilities.PlayableMediaTypes == 0)
assert(#pushed.PlayableMediaTypes == 0 and string.find(encoded, '"PlayableMediaTypes":[]', 1, true), encoded)
assert(full({}).status == 204)
encoded = websocket.encode("Sessions", listed_sessions(), nil)
assert(not string.find(encoded, '"Capabilities":[', 1, true), encoded)
assert(serde.json.decode(encoded).Data[1].Capabilities.SupportsPersistentIdentifier == false, "the body form's default")

-- A reconnect finds the latest declaration, kept in the store rather than the socket.
assert(partial({ playableMediaTypes = "Audio", supportsMediaControl = "true" }).status == 204)
disconnect(connection)
connection = connect()
assert(#connection.entry.capabilities.SupportedCommands == 0, "the replaced list stays replaced")
assert(connection.entry.capabilities.PlayableMediaTypes[1] == "Audio")
assert(listed_socket(connection).SupportsRemoteControl == true, "a declared socket is a cast target")
disconnect(connection)

local store = datastore.get_or_create("session_capabilities")
local stored: any = store:get(connection.entry.session_id)
assert(stored and stored.user_id == fixture.users.listener and stored.capabilities.SupportsMediaControl == true)

-- A declaration kept for another user isn't given to this one's socket.
store:set(connection.entry.session_id, { user_id = fixture.users.other, capabilities = stored.capabilities })
connection = connect()
assert(connection.entry.capabilities == nil, "another user's stays theirs")
disconnect(connection)

-- A socket that declared nothing is no cast target, until it does.
local undeclared =
	connect(`MediaBrowser Client="Finamp", Device="Tab", DeviceId="tab", Version="1", Token="{fixture.api_keys.phone}"`)
dto = listed_socket(undeclared)
assert(dto.SupportsRemoteControl == false and dto.SupportsMediaControl == false)
assert(#dto.SupportedCommands == 0 and #dto.PlayableMediaTypes == 0)
disconnect(undeclared)

-- A declaration is bounded when it is read.
local long = {}
for index = 1, session.MAX_CAPABILITY_LIST_LENGTH + 10 do
	long[index] = `Command{index}`
end
long[1] = string.rep("x", session.MAX_CAPABILITY_STRING_LENGTH + 1)
local bounded: any = session.parse_client_capabilities({
	SupportedCommands = long,
	IconUrl = string.rep("x", session.MAX_CAPABILITY_STRING_LENGTH + 1),
} :: any)
assert(#bounded.SupportedCommands == session.MAX_CAPABILITY_LIST_LENGTH)
assert(bounded.SupportedCommands[1] == "Command2", "an overlong entry is dropped")
assert(bounded.IconUrl == nil)
local csv = table.concat(table.create(session.MAX_CAPABILITY_LIST_LENGTH + 10, "Play"), ",")
local from_query: any = session.parse_client_capabilities_query({ SupportedCommands = { csv } } :: any)
assert(#from_query.SupportedCommands == session.MAX_CAPABILITY_LIST_LENGTH, "the query form too")
