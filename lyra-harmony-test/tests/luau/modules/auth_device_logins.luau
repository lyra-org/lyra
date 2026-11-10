local fixture = ...
local auth = require("@lyra/auth")
local errors = require("@lyra/errors")

local manager = fixture.users.manager
local listener = fixture.users.listener
local owner = fixture.users.owner

local function begin(options: any?): any
	local outcome = auth.begin_device_login(options)
	assert(outcome.status == "ok", outcome.status)
	assert(outcome.retry_after_seconds == nil)
	return assert(outcome.result)
end

-- Approvals bind the credential the dispatch resolved.
assert(auth.resolve_auth(fixture.sessions.manager_phone) ~= nil)

-- Letters by default, without I, L or O; a caller's own code is stored tidied and held once.
local letters = begin({ client_name = " TV \0 ", user_agent = "Agent" })
assert(string.match(letters.code, "^[A-HJKMNP-Z]+$") and #letters.code == 6, letters.code)
assert(#letters.secret == 64)
assert(letters.expires_at_ms - letters.created_at_ms == 600000)
local given = begin({ code = " 123 abc " })
assert(given.code == "123ABC", given.code)
local taken = auth.begin_device_login({ code = "123abc" })
assert(taken.status == "code_taken" and taken.result == nil)
assert(auth.device_login_status(given.secret).status == "pending")

assert(not pcall(auth.begin_device_login, { code = "12345" }))
assert(not pcall(auth.begin_device_login, { code = "123-456" }))
assert(not pcall(auth.begin_device_login, { format = "digits" }))
assert(not pcall(auth.approve_device_login, letters.code, { userId = manager }))

-- Status reads and never signs in, a claim waits, and neither knows a stranger.
local waiting = auth.device_login_status(letters.secret)
assert(waiting.status == "pending")
assert(assert(waiting.result).code == letters.code)
assert(assert(waiting.result).client_name == "TV")
assert(assert(waiting.result).created_at_ms == letters.created_at_ms)
local pending_claim = auth.claim_device_login(letters.secret)
assert(pending_claim.status == "pending" and pending_claim.result == nil)
assert(auth.device_login_status("no-such-secret").status == "not_found")
assert(auth.device_login_status("no-such-secret").result == nil)
assert(auth.claim_device_login("no-such-secret").status == "not_found")

-- Approval ignores case and whitespace, and the first one wins.
local spaced = ` {string.sub(letters.code, 1, 3)} {string.lower(string.sub(letters.code, 4))} `
assert(auth.approve_device_login(spaced).status == "ok")
assert(auth.approve_device_login(letters.code).status == "already_approved")
assert(auth.approve_device_login("NOSUCH").status == "not_found")
assert(auth.device_login_status(letters.secret).status == "approved")

-- A claim is what a login gives, and repeats give the same session.
local claimed = auth.claim_device_login(letters.secret)
assert(claimed.status == "ok")
local login = assert(claimed.result)
assert(login.principal.user_id == manager and login.principal.username ~= nil)
assert(type(login.token) == "string")
assert(assert(auth.resolve_auth(login.token)).principal.user_id == manager)
local retried = auth.claim_device_login(letters.secret)
assert(retried.status == "ok")
assert(assert(retried.result).token == login.token)
assert(auth.device_login_status(letters.secret).status == "approved")

-- The session carries the client the device login began with.
local labelled = false
for _, session in auth.list_sessions() do
	if session.client_name == "TV" then
		labelled = session.user_agent == "Agent"
	end
end
assert(labelled, "the claimed session keeps the client's label and agent")

-- Approving for someone else takes manage_users and every permission they hold.
local for_listener = begin()
assert(auth.approve_device_login(for_listener.code, { user_id = listener }).status == "ok")
local listener_login = auth.claim_device_login(for_listener.secret)
assert(assert(listener_login.result).principal.user_id == listener)

local for_owner = begin()
local refused, refusal = pcall(auth.approve_device_login, for_owner.code, { user_id = owner })
assert(not refused and errors.kind(refusal) == "forbidden", tostring(refusal))
assert(auth.device_login_status(for_owner.secret).status == "pending")

assert(auth.approve_device_login(for_owner.code, { user_id = "no-such-user" }).status == "user_not_found")
assert(auth.approve_device_login(for_owner.code, { user_id = manager }).status == "ok")

-- Data stays with the device login for whoever holds the secret, through the claim and its repeats.
local identity = { device = "Pixel", version = "1" }
local with_data = begin({ data = identity })
local read_back = assert(auth.device_login_status(with_data.secret).result).data :: any
assert(read_back.device == "Pixel" and read_back.version == "1")
assert(assert(auth.device_login_status(for_owner.secret).result).data == nil)
assert(auth.approve_device_login(with_data.code).status == "ok")
for _ = 1, 2 do
	local with_data_claim = auth.claim_device_login(with_data.secret)
	assert(with_data_claim.status == "ok" and (with_data_claim.data :: any).device == "Pixel")
end
assert(auth.claim_device_login("no-such-secret").data == nil)
assert(not pcall(auth.begin_device_login, { data = string.rep("x", auth.MAX_DEVICE_LOGIN_DATA_BYTES) }))

-- An API key signs no one in.
local for_key = begin()
assert(auth.resolve_auth(fixture.api_keys.manager_script) ~= nil)
local key_approved, key_refusal = pcall(auth.approve_device_login, for_key.code)
assert(not key_approved and errors.kind(key_refusal) == "forbidden", tostring(key_refusal))
assert(auth.device_login_status(for_key.secret).status == "pending")
